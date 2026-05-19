from machine import Pin
from sx1262 import SX1262
import time, _thread, random, network, socket, json
from beacon_protocol import BeaconPacket, ControlPacket, DataPacket, JoinReqPacket, HubSchedPacket, \
     TYPE_BEACON, TYPE_CONTROL, TYPE_DATA_REQ, TYPE_JOIN_REQ, TYPE_HUB_SCHED, TYPE_MSG_CHUNK, TYPE_FILE_CHUNK, TYPE_ACK
from slot_manager import SlotManager
from config_loader import load_identity
from utils2 import get_network_time, set_network_time, log, web_logs

# --- CONFIG ---
id_data = load_identity()
MY_ADDR = id_data.get("my_addr", 0x02)

# --- QUICK UDP & SOS SETTINGS ---
MAX_RETRIES = 2
MSG_TTL = 120000   
OUTBOX_LIMIT = 10
DEDUP_TTL = 10000

SOS_FREQ = 868.50  
TYPE_SOS = 0x99    
pending_sos_msgs = [] 

recent_packets = {}

# --- THREADING LOCK ---
tx_lock = _thread.allocate_lock() # 🔒 Prevents SPI collisions between TDMA and SOS threads

# --- STATE VARIABLES ---
current_role = "LISTENER" 
sync_source = "UNSYNCED"  
last_beacon_time = time.ticks_ms()
WATCHDOG_TIMEOUT = 80000 + (MY_ADDR * 2000) 
missed_beacons = 0  

# --- GPS AND FRAGMENTATION STATE ---
my_lat = 0.0
my_lon = 0.0
node_locations = {} 
current_peer = 1 

CHUNK_SIZE = 50 
msg_id_counter = 0
outbox = {}  
inbox = {}   
ack_queue = [] 
received_messages = []

sm = SlotManager(MY_ADDR)
active_nodes = []
pending_reqs = []
is_joined = False
last_phase = ""

log(f"[BOOT] Node 0x{MY_ADDR:02X} starting. Waiting for Phone Sync or Hub Beacon...", save_to_file=True)

# --- HARDWARE SETUP ---
FREQ_PAIRS = {i: (865.10 + i*0.15, 866.10 + i*0.15) for i in range(6)}

def get_sx(spi_bus, clk, mosi, miso, cs, irq, rst, gpio, f):
    obj = SX1262(
        spi_bus=spi_bus, clk=Pin(clk), mosi=Pin(mosi), miso=Pin(miso),
        cs=Pin(cs), irq=Pin(irq), rst=Pin(rst), gpio=Pin(gpio)   
    )
    obj.begin(freq=f, bw=125.0, sf=7, cr=5, syncWord=0x1424, power=22)
    return obj

tx_f, rx_f = FREQ_PAIRS[0]
last_tx_f = tx_f
target_rx_f = rx_f  

log(f"[HW INIT] Full-Duplex Radios Online -> TX: {tx_f:.2f} MHz | RX: {rx_f:.2f} MHz")

sx_tx = get_sx(spi_bus=1, clk=12, mosi=13, miso=14, cs=11, irq=17, rst=15, gpio=16, f=tx_f)
sx_rx = get_sx(spi_bus=2, clk=33, mosi=34, miso=35, cs=18, irq=38, rst=36, gpio=37, f=rx_f)

def log_data_packet(event, unique_id, seq, target_node):
    now_ts = get_network_time()
    log_line = f"{now_ts},{event},{unique_id},{seq},0x{target_node:02X}\n"
    try:
        with open("data.log", "a") as f: f.write(log_line)
    except: pass 

def switch_lane(p, peer_addr=None):
    global last_tx_f
    t, _ = FREQ_PAIRS[p]
    if p == 0:
        if current_role == "HUB": t = FREQ_PAIRS[p][1] 
    else:
        if peer_addr is not None:
            if MY_ADDR > peer_addr: t = FREQ_PAIRS[p][1]
        elif current_role == "HUB": t = FREQ_PAIRS[p][1]

    if t != last_tx_f:
        with tx_lock: # 🔒 Thread-safe frequency change
            sx_tx.setFrequency(t)
        last_tx_f = t
        log(f"[FREQ] TX Chip strictly tuned to {t:.2f} MHz (Lane {p})", save_to_file=False) 
        time.sleep_ms(15)

def csma_backoff():
    time.sleep_ms(random.randint(300, 4500))

def generate_random_string(length):
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choice(chars) for _ in range(length))

# --- 🚀 NEW DEDICATED SOS THREAD ---
def sos_thread_loop():
    global last_tx_f
    while True:
        if pending_sos_msgs:
            msg_text = pending_sos_msgs.pop(0)
            sos_pkt = DataPacket(0xFF, MY_ADDR, 0, 0, 1, TYPE_SOS, msg_text.encode('utf-8'))
            
            with tx_lock: # 🔒 Safely hijack the TX chip
                original_f = last_tx_f # Remember where the TDMA engine was
                
                sx_tx.setFrequency(SOS_FREQ) # Tune to emergency freq
                time.sleep_ms(15)
                sx_tx.send(sos_pkt.to_bytes()) # Blast SOS
                
                sx_tx.setFrequency(original_f) # Instantly restore TDMA freq
                time.sleep_ms(15)
                
            log(f"🚨 [SOS TX] Broadcasted emergency message: {msg_text}", save_to_file=True)
            
        time.sleep_ms(50) # Low CPU poll rate

# --- TX ENGINE ---
def sender_loop():
    global current_role, sync_source, is_joined, last_phase, missed_beacons, current_peer, ack_queue, outbox, target_rx_f, msg_id_counter, pending_reqs, last_tx_f
    flags = {"b":0, "c":0, "r":0, "s":0, "end_ping": 0}
    
    while True:
        try:
            now_ticks = time.ticks_ms()

            to_delete = []
            for mid, mdata in outbox.items():
                if time.ticks_diff(now_ticks, mdata["created_at"]) > mdata["ttl"]:
                    log(f"[DROP] TTL expired {MY_ADDR:02X}-{mid}", save_to_file=True)
                    to_delete.append(mid)
            for mid in to_delete: del outbox[mid]

            if sync_source == "UNSYNCED":
                switch_lane(0); time.sleep_ms(100); continue 

            if current_role == "LISTENER":
                switch_lane(0)
                if (now_ticks - last_beacon_time) > WATCHDOG_TIMEOUT:
                    log(f"[STATE] WATCHDOG TRIGGERED! Promoting to HUB.", save_to_file=True)
                    current_role = "HUB"; sync_source = "SELF (HUB)"; active_nodes.clear(); active_nodes.append(MY_ADDR)
                    target_rx_f = FREQ_PAIRS[0][0] 
                    is_joined = True; now_net = get_network_time(); set_network_time(now_net - (now_net % 60000)); last_phase = "" 
                time.sleep_ms(100); continue 

            sm.update()
            tis = sm.time_in_slot
            curr_phase = sm.get_current_phase()

            if curr_phase != last_phase:
                log(f"--- Transitioning to {curr_phase} ---", save_to_file=False)
                
                if curr_phase == "1. BEACON (SYNC)":
                    flags = {k:0 for k in flags}; sm.assigned_lane = 0 
                    target_rx_f = SOS_FREQ if current_role == "HUB" else FREQ_PAIRS[0][1]

                    for mid, mdata in outbox.items():
                        mdata["priority"] = min(mdata["priority"] + 1, 5)
                        
                    if is_joined and len(active_nodes) > 1 and len(outbox) < OUTBOX_LIMIT:
                        valid_targets = [n for n in active_nodes if n != MY_ADDR]
                        if valid_targets:
                            target_id = random.choice(valid_targets)
                            msg_text = f"TST_{MY_ADDR:02X}_" + generate_random_string(18) 
                            msg_bytes = msg_text.encode('utf-8')
                            
                            total_chunks = (len(msg_bytes) + CHUNK_SIZE - 1) // CHUNK_SIZE
                            msg_id_counter = (msg_id_counter + 1) % 256
                            unique_log_id = f"{MY_ADDR:02X}-{msg_id_counter}"
                            
                            chunks_dict = {i: {"data": msg_bytes[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE], "state": "UNSENT", "retries": 0, "ts": 0} for i in range(total_chunks)}
                            
                            outbox[msg_id_counter] = {
                                "to": target_id, "priority": 1, "chunks": chunks_dict, 
                                "total": total_chunks, "created_at": time.ticks_ms(), "ttl": MSG_TTL
                            }
                            
                            for i in range(total_chunks): log_data_packet("CREATED", unique_log_id, i, target_id)
                            log(f"🧪 [STRESS TEST] Queued msg for 0x{target_id:02X}. [UNIQUE_ID: {unique_log_id} | PRIO: 1]", save_to_file=True)

                elif curr_phase == "2. CONTROL/JOIN":
                    target_rx_f = FREQ_PAIRS[0][0] if current_role == "HUB" else SOS_FREQ
                    if current_role == "CLIENT":
                        if (time.ticks_ms() - last_beacon_time) > 20000:
                            missed_beacons += 1
                            if missed_beacons > MY_ADDR: 
                                log("[STATE] HUB LOST! Promoting to HUB.", save_to_file=True)
                                current_role = "HUB"; sync_source = "SELF (HUB)"; active_nodes.clear(); active_nodes.append(MY_ADDR)
                                target_rx_f = FREQ_PAIRS[0][0]
                                is_joined = True; now_net = get_network_time(); set_network_time(now_net - (now_net % 60000))
                        else: missed_beacons = 0 

                elif curr_phase == "3. DATA REQUEST":
                    target_rx_f = FREQ_PAIRS[0][0] if current_role == "HUB" else SOS_FREQ

                elif curr_phase == "3.5 SCHEDULING":
                    target_rx_f = SOS_FREQ if current_role == "HUB" else FREQ_PAIRS[0][1]

                last_phase = curr_phase

            if tis < sm.PHASE_BEACON_END:
                switch_lane(0)
                if current_role == "HUB" and not flags["b"]:
                    now_net = get_network_time()
                    with tx_lock: sx_tx.send(BeaconPacket(MY_ADDR, now_net, now_net - (now_net % 60000), 4-sm.slot_idx, active_nodes).to_bytes())
                    flags["b"] = 1
                    log(f"[TX] Beacon Sent (Active Nodes: {len(active_nodes)})", save_to_file=False)

            elif tis < sm.PHASE_CONTROL_END:
                if current_role == "CLIENT" and not flags["c"]:
                    csma_backoff() 
                    if not is_joined:
                        with tx_lock: sx_tx.send(JoinReqPacket(MY_ADDR, my_lat, my_lon).to_bytes())
                        flags["c"] = 1
                        log(f"[TX] Join Request Sent", save_to_file=True)
                    else:
                        with tx_lock: sx_tx.send(ControlPacket(MY_ADDR, my_lat, my_lon).to_bytes())
                        flags["c"] = 1
                        log(f"[TX] Heartbeat Sent", save_to_file=False)

            elif tis < sm.PHASE_DATAREQ_END:
                if current_role == "CLIENT" and is_joined and not flags["r"]:
                    best_target = None; best_prio = -1
                    for mid, mdata in outbox.items():
                        if mdata["to"] not in active_nodes: continue 
                        for seq, chunk in mdata["chunks"].items():
                            if chunk["state"] == "UNSENT" or (chunk["state"] == "IN_FLIGHT" and time.ticks_ms() - chunk["ts"] > 4000):
                                if mdata["priority"] > best_prio:
                                    best_prio = mdata["priority"]; best_target = mdata["to"]
                                break
                    if best_target is not None:
                        time.sleep_ms(random.randint(500, 3000)) 
                        hub_addr = int(sync_source.split("0x")[1].replace(")", ""), 16) if "0x" in sync_source else 1
                        with tx_lock: sx_tx.send(DataPacket(hub_addr, MY_ADDR, best_target, best_prio, 0, TYPE_DATA_REQ, b'').to_bytes())
                        log(f"[TX] Requesting lane for Node 0x{best_target:02X} (Priority: {best_prio}).", save_to_file=True)
                    flags["r"] = 1

            elif tis < sm.PHASE_SCHED_END:
                if current_role == "HUB" and not flags["s"]:
                    pending_reqs = [pr for pr in pending_reqs if pr[0] in active_nodes and pr[1] in active_nodes]
                    pending_reqs.sort(key=lambda x: x[2], reverse=True)

                    asgn = []; used_nodes = set(); lane = 1
                    
                    for src, tgt, prio in pending_reqs:
                        if src in used_nodes or tgt in used_nodes: continue
                        asgn.append((src, lane))
                        if tgt != src: asgn.append((tgt, lane))
                        used_nodes.add(src); used_nodes.add(tgt)
                        lane += 1
                        if lane > 5: break

                    with tx_lock: sx_tx.send(HubSchedPacket(asgn).to_bytes())
                    
                    my_lane = next((l for a, l in asgn if a == MY_ADDR), 0)
                    sm.assigned_lane = my_lane
                    if sm.assigned_lane > 0:
                        current_peer = next((a for a, l in asgn if l == my_lane and a != MY_ADDR), 1)
                        _, next_r = FREQ_PAIRS[sm.assigned_lane]
                        if MY_ADDR > current_peer: next_r = FREQ_PAIRS[sm.assigned_lane][0]
                        target_rx_f = next_r
                        
                    pending_reqs.clear(); flags["s"] = 1
                    readable_asgn = [f"Node 0x{n:02X} -> Lane {l}" for n, l in asgn]
                    log(f"[TX] Schedule Broadcasted. Assignments: {readable_asgn}", save_to_file=False)
                
                if current_role == "CLIENT" and not flags["s"] and sm.assigned_lane > 0:
                    time.sleep_ms(1500) 
                    with tx_lock: sx_tx.send(ControlPacket(MY_ADDR, my_lat, my_lon).to_bytes())
                    flags["s"] = 1

            # --- PHASE 4: DATA TRANSFER ---
            else:
                if sm.assigned_lane > 0:
                    switch_lane(sm.assigned_lane, peer_addr=current_peer)
                    
                    if len(ack_queue) > 0:
                        ack_tgt, ack_mid, ack_seq = ack_queue.pop(0)
                        with tx_lock: sx_tx.send(DataPacket(ack_tgt, MY_ADDR, ack_mid, ack_seq, 0, TYPE_ACK, b'').to_bytes())
                        unique_ack_id = f"{ack_tgt:02X}-{ack_mid}"
                        log(f"[ACK] ✉️ Sent Receipt for [UNIQUE_ID: {unique_ack_id} | SEQ: {ack_seq+1}] to 0x{ack_tgt:02X}", save_to_file=True)
                        time.sleep_ms(10) 
                    
                    else:
                        chunk_sent = False
                        for mid, mdata in outbox.items():
                            if mdata["to"] != current_peer: continue
                            for seq, chunk in mdata["chunks"].items():
                                if chunk["state"] == "UNSENT" or (chunk["state"] == "IN_FLIGHT" and time.ticks_ms() - chunk["ts"] > 3000):
                                    unique_log_id = f"{MY_ADDR:02X}-{mid}"
                                    if chunk["retries"] >= MAX_RETRIES:
                                        log(f"[WARN] Dropping [UNIQUE_ID: {unique_log_id} | SEQ: {seq+1}] after max retries.", save_to_file=True)
                                        chunk["state"] = "FAILED"
                                        continue
                                        
                                    if chunk["retries"] > 0: time.sleep_ms(random.randint(200, 1000))
                                        
                                    retry_lbl = "[RETRY] " if chunk["retries"] > 0 else "🚀 "
                                    with tx_lock: sx_tx.send(DataPacket(current_peer, MY_ADDR, mid, seq, mdata["total"], TYPE_MSG_CHUNK, chunk["data"]).to_bytes())
                                    log_data_packet("SENT", unique_log_id, seq, current_peer)
                                    log(f"[TX] {retry_lbl}Fired chunk [UNIQUE_ID: {unique_log_id} | SEQ: {seq+1}/{mdata['total']}] to 0x{current_peer:02X}", save_to_file=True)
                                    chunk["state"] = "IN_FLIGHT"
                                    chunk["ts"] = time.ticks_ms()
                                    chunk["retries"] += 1
                                    chunk_sent = True
                                    time.sleep_ms(50) 
                                    break 
                            if chunk_sent: break

                    if tis > 58500 and not flags.get("end_ping"):
                        time.sleep_ms(random.randint(10, 400)) 
                        target_rx_f = FREQ_PAIRS[0][0] if current_role == "HUB" else FREQ_PAIRS[0][1]
                        with tx_lock: sx_tx.send(ControlPacket(MY_ADDR, my_lat, my_lon).to_bytes())
                        flags["end_ping"] = 1
                        log("[TX] 🔔 End-of-Frame Ping fired to safely tune RX Coma.", save_to_file=False)

                else: 
                    target_rx_f = SOS_FREQ
                    switch_lane(0)

            time.sleep_ms(50)
        except Exception as e: log(f"TX Error: {e}")

# --- RX ENGINE ---
def rx_loop():
    global current_role, sync_source, is_joined, last_beacon_time, target_rx_f, current_peer, ack_queue, inbox, outbox, received_messages, pending_reqs, recent_packets
    current_rx_f = FREQ_PAIRS[0][1] 
    
    while True:
        try:
            if current_rx_f != target_rx_f:
                sx_rx.setFrequency(target_rx_f)
                current_rx_f = target_rx_f
                log(f"[FREQ] RX Chip securely tuned to {current_rx_f:.2f} MHz", save_to_file=False)
                time.sleep_ms(15)

            data, _ = sx_rx.recv(timeout_ms=20) 
            
            if data:
                t = data[0]
                if t == TYPE_BEACON:
                    b = BeaconPacket.from_bytes(data)
                    if b:
                        last_beacon_time = time.ticks_ms()
                        if current_role != "HUB":
                            set_network_time(b.net_time) 
                            sm.time_in_slot = b.net_time - b.frame_start
                            sync_source = f"HUB (0x{b.hub_id:02X})"
                            if current_role == "LISTENER":
                                log(f"[STATE] Heard Hub 0x{b.hub_id:02X}. Switching to CLIENT role.", save_to_file=True)
                                current_role = "CLIENT"
                                target_rx_f = FREQ_PAIRS[0][1] 
                            active_nodes.clear(); active_nodes.extend(b.active_nodes)
                            if MY_ADDR in active_nodes and not is_joined:
                                is_joined = True; log("[STATE] Successfully joined the network!", save_to_file=True)
                            
                elif t == TYPE_JOIN_REQ and current_role == "HUB":
                    j = JoinReqPacket.from_bytes(data)
                    if j:
                        if j.node_addr not in active_nodes: active_nodes.append(j.node_addr)
                        node_locations[j.node_addr] = {"lat": j.lat, "lon": j.lon} 
                        
                elif t == TYPE_CONTROL and current_role == "HUB":
                    c = ControlPacket.from_bytes(data)
                    if c: node_locations[c.src] = {"lat": c.lat, "lon": c.lon} 
                        
                elif t == TYPE_DATA_REQ and current_role == "HUB":
                    d = DataPacket.from_bytes(data)
                    if d:
                        target = d.msg_id 
                        priority = d.seq_num
                        req_tuple = (d.from_addr, target, priority)
                        
                        pending_reqs = [pr for pr in pending_reqs if pr[0] != d.from_addr]
                        pending_reqs.append(req_tuple)
                        log(f"[HUB] Data Request caught. Node 0x{d.from_addr:02X} wants to msg Node 0x{target:02X} (Prio: {priority})", save_to_file=False)
                        
                elif t == TYPE_HUB_SCHED and current_role == "CLIENT":
                    assignments = []
                    try:
                        s = HubSchedPacket.from_bytes(data)
                        assignments = s.assignments if s else []
                    except Exception as e:
                        log(f"[WARN] Schedule parsing failed: {e}. Utilizing manual payload extraction.", save_to_file=True)
                        num_asgn = data[1]
                        idx = 2
                        for _ in range(num_asgn):
                            if idx + 1 < len(data):
                                assignments.append((data[idx], data[idx+1]))
                                idx += 2
                                
                    sm.assigned_lane = next((l for a, l in assignments if a == MY_ADDR), 0)
                    if sm.assigned_lane > 0:
                        current_peer = next((a for a, l in assignments if l == sm.assigned_lane and a != MY_ADDR), 1)
                        _, next_r = FREQ_PAIRS[sm.assigned_lane]
                        if MY_ADDR > current_peer: next_r = FREQ_PAIRS[sm.assigned_lane][0]
                        target_rx_f = next_r
                        log(f"[RX] Hub assigned Lane {sm.assigned_lane}. Pre-tuning RX to {target_rx_f:.2f} MHz!", save_to_file=True)

                elif t == TYPE_SOS:
                    d = DataPacket.from_bytes(data)
                    if d:
                        msg_text = d.payload.decode('utf-8')
                        log(f"🚨 [SOS RX] Emergency Alert from 0x{d.from_addr:02X}: {msg_text}", save_to_file=True)
                        received_messages.append({"from": f"🚨 SOS-0x{d.from_addr:02X}", "msg": msg_text})
                        if len(received_messages) > 15: received_messages.pop(0) 

                elif t == TYPE_MSG_CHUNK or t == TYPE_FILE_CHUNK:
                    d = DataPacket.from_bytes(data)
                    if d and d.to_addr == MY_ADDR:
                        unique_log_id = f"{d.from_addr:02X}-{d.msg_id}"
                        
                        pkt_key = (d.from_addr, d.msg_id, d.seq_num)
                        now = time.ticks_ms()
                        for k in list(recent_packets.keys()):
                            if time.ticks_diff(now, recent_packets[k]) > DEDUP_TTL: del recent_packets[k]
                        if pkt_key in recent_packets: continue
                        recent_packets[pkt_key] = now
                        
                        log(f"[RX] 📥 Caught chunk [UNIQUE_ID: {unique_log_id} | SEQ: {d.seq_num+1}/{d.total_chunks}] from 0x{d.from_addr:02X}. Buffering...", save_to_file=True)
                        
                        if d.msg_id not in inbox:
                            inbox[d.msg_id] = {"from": d.from_addr, "total": d.total_chunks, "chunks": {}}
                        inbox[d.msg_id]["chunks"][d.seq_num] = d.payload
                        
                        if d.seq_num == d.total_chunks - 1:
                            if (d.from_addr, d.msg_id, d.seq_num) not in ack_queue:
                                ack_queue.append((d.from_addr, d.msg_id, d.seq_num))

                        if len(inbox[d.msg_id]["chunks"]) == d.total_chunks:
                            full_payload = b"".join([inbox[d.msg_id]["chunks"][i] for i in range(d.total_chunks)])
                            try: msg_text = full_payload.decode('utf-8')
                            except Exception: msg_text = f"<Decoded with errors> {str(full_payload)}"
                            
                            log(f"🟢 [REASSEMBLY COMPLETE] Msg from 0x{d.from_addr:02X}: {msg_text}", save_to_file=True)
                            received_messages.append({"from": hex(d.from_addr), "msg": msg_text})
                            if len(received_messages) > 15: received_messages.pop(0) 
                            del inbox[d.msg_id]

                elif t == TYPE_ACK:
                    d = DataPacket.from_bytes(data)
                    if d and d.to_addr == MY_ADDR:
                        unique_log_id = f"{MY_ADDR:02X}-{d.msg_id}"
                        log_data_packet("ACK_RCVD", unique_log_id, d.seq_num, d.from_addr)
                        
                        log(f"[ACK] ✉️ Receipt Caught for [UNIQUE_ID: {unique_log_id} | SEQ: {d.seq_num+1}]. Marking complete.", save_to_file=True)
                        if d.msg_id in outbox and d.seq_num in outbox[d.msg_id]["chunks"]:
                            outbox[d.msg_id]["chunks"][d.seq_num]["state"] = "ACKED"
                            
                        if d.msg_id in outbox and all(c["state"] in ["ACKED", "FAILED"] for c in outbox[d.msg_id]["chunks"].values()):
                            log(f"[FRAG] UNIQUE_ID: {unique_log_id} fully processed. Clearing from outbox.", save_to_file=True)
                            del outbox[d.msg_id]
                            
        except Exception as e: 
            log(f"[RX ERROR] Engine failed cycle: {e}")
            time.sleep_ms(100) 

# --- START ALL THREE THREADS ---
_thread.start_new_thread(rx_loop, ())
_thread.start_new_thread(sender_loop, ())
_thread.start_new_thread(sos_thread_loop, ()) # 🚀 Boot the new thread!

def unquote(s):
    res = s.split('%'); out = res[0]
    for item in res[1:]:
        if len(item) >= 2:
            try: out += chr(int(item[:2], 16)) + item[2:]
            except: out += '%' + item
        else: out += '%' + item
    return out

def run_web():
    global sync_source, my_lat, my_lon, msg_id_counter, outbox, received_messages, last_tx_f
    w = network.WLAN(network.AP_IF); w.active(True)
    ssid = f"NODE_{hex(MY_ADDR).upper().replace('0X', '')}_NET"
    w.config(essid=ssid, authmode=0)

    s = socket.socket(); s.bind(('', 80)); s.listen(3)
    while True:
        try:
            cl, _ = s.accept(); cl.settimeout(2.0)
            r = cl.recv(1024).decode()
            
            if "/api/set_time" in r:
                try:
                    query = r.split(" /api/set_time?")[1].split(" ")[0]
                    params = dict(p.split('=') for p in query.split('&'))
                    if "epoch" in params and sync_source == "UNSYNCED":
                        set_network_time(int(params["epoch"]))
                        sync_source = "PHONE (NTP)"  
                        log(f"[SYNC] Time perfectly synchronized with Phone: {params['epoch']}", save_to_file=True)
                    if "lat" in params and "lon" in params:
                        my_lat = float(params["lat"]); my_lon = float(params["lon"])
                        log(f"[GPS] Coordinates acquired: {my_lat:.6f}, {my_lon:.6f}", save_to_file=True)
                except Exception as e: pass
                cl.send("HTTP/1.1 200 OK\r\n\r\nOK")

            elif "/api/send_sos" in r:
                try:
                    query = r.split(" /api/send_sos?")[1].split(" ")[0]
                    params = dict(p.split('=') for p in query.split('&'))
                    if "msg" in params:
                        msg_text = unquote(params["msg"])
                        pending_sos_msgs.append(msg_text)
                        log(f"[WEB] 🚨 SOS Alert accepted and safely queued for background broadcast.", save_to_file=True)
                except Exception as e: pass
                cl.send("HTTP/1.1 200 OK\r\n\r\nOK")

            elif "/api/send_msg" in r:
                try:
                    query = r.split(" /api/send_msg?")[1].split(" ")[0]
                    params = dict(p.split('=') for p in query.split('&'))
                    if "to" in params and "msg" in params:
                        target_id = int(params["to"])
                        if target_id == MY_ADDR:
                            log("[WEB] Cannot send message to self. Discarded.", save_to_file=True)
                        else:
                            if len(outbox) > OUTBOX_LIMIT:
                                log("[WEB] Outbox full, dropping new message.", save_to_file=True)
                                cl.send("HTTP/1.1 429 Too Many Requests\r\n\r\nOK")
                                continue

                            msg_text = unquote(params["msg"])
                            msg_bytes = msg_text.encode('utf-8')
                            total_chunks = (len(msg_bytes) + CHUNK_SIZE - 1) // CHUNK_SIZE
                            msg_id_counter = (msg_id_counter + 1) % 256
                            unique_log_id = f"{MY_ADDR:02X}-{msg_id_counter}"
                            
                            chunks_dict = {}
                            for i in range(total_chunks):
                                chunk_data = msg_bytes[i*CHUNK_SIZE : (i+1)*CHUNK_SIZE]
                                chunks_dict[i] = {"data": chunk_data, "state": "UNSENT", "retries": 0, "ts": 0}
                                log_data_packet("CREATED", unique_log_id, i, target_id)
                                
                            outbox[msg_id_counter] = {
                                "to": target_id, "priority": 1, "chunks": chunks_dict, 
                                "total": total_chunks, "created_at": time.ticks_ms(), "ttl": MSG_TTL
                            }
                            log(f"[WEB] Received message for 0x{target_id:02X}. Sliced {len(msg_bytes)} bytes into {total_chunks} chunk(s) [UNIQUE_ID: {unique_log_id}]", save_to_file=True)
                except Exception as e: pass
                cl.send("HTTP/1.1 200 OK\r\n\r\nOK")

            elif "/api/state" in r:
                res = {
                    "addr": hex(MY_ADDR), "role": current_role, "sync": sync_source,
                    "phase": sm.get_current_phase(), "slot": sm.slot_idx+1, 
                    "active": [hex(n) for n in active_nodes], 
                    "locations": node_locations,
                    "messages": received_messages,
                    "logs": web_logs
                }
                cl.send("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n" + json.dumps(res))
            else:
                with open("index.html", "r") as f: 
                    cl.send("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" + f.read())
            cl.close()
        except: 
            try: cl.close()
            except: pass

run_web()
