# ===== Monitoring helpers =====
SAMPLE_INTERVAL=1   # seconds

# Resolve block device for a given path (e.g., /mnt/RTC -> nvme0n1)
get_block_device_for_path() {
  local path="$1"
  local dev
  dev=$(df -P "$path" | awk 'NR==2{print $1}')
  # If it's a mapper or partition, map to parent device name for iostat
  if command -v lsblk >/dev/null 2>&1; then
    # PKNAME gives parent (e.g., nvme0n1p2 -> nvme0n1, dm-0 -> nvme0n1)
    local pk
    pk=$(lsblk -no PKNAME "$dev" 2>/dev/null | head -n1)
    if [ -n "$pk" ]; then
      echo "$pk"
      return
    fi
  fi
  # Fallback: strip partition suffixes (best-effort)
  basename "$dev" | sed -E 's/p?[0-9]+$//'
}

# ===== Monitoring helpers =====
SAMPLE_INTERVAL=1   # seconds

# Resolve block device for a given path (e.g., /mnt/RTC -> nvme0n1)
get_block_device_for_path() {
  local path="$1"
  local dev
  dev=$(df -P "$path" | awk 'NR==2{print $1}')
  # If it's a mapper or partition, map to parent device name for iostat
  if command -v lsblk >/dev/null 2>&1; then
    # PKNAME gives parent (e.g., nvme0n1p2 -> nvme0n1, dm-0 -> nvme0n1)
    local pk
    pk=$(lsblk -no PKNAME "$dev" 2>/dev/null | head -n1)
    if [ -n "$pk" ]; then
      echo "$pk"
      return
    fi
  fi
  # Fallback: strip partition suffixes (best-effort)
  basename "$dev" | sed -E 's/p?[0-9]+$//'
}

start_monitors() {
  local pid="$1"           # db_bench PID
  local out_prefix="$2"    # e.g., /home/.../out_15M_uni0_rtc1_round1
  local db_path="$3"       # db_dir to deduce device
  local dev_override="${4:-}"  # optional: pass device manually (e.g., nvme0n1)

  # Choose device
  local disk="${dev_override}"
  [ -z "$disk" ] && disk="$(get_block_device_for_path "$db_path")"

  echo "[MON] Using disk device for iostat: ${disk}"

  # pidstat (CPU, MEM, IO) for the db_bench PID
  #   -h: no headers (easier CSV), -r: memory, -u: CPU, -d: IO, -p: PID, interval 1s
  # We convert spaces to commas and prepend timestamp.
LC_ALL=C pidstat -h -r -u -d -p "$pid" $SAMPLE_INTERVAL | awk -v OFS=',' -v nowfmt="$(date +%Y-%m-%dT%H:%M:%S)" '
  BEGIN{print "timestamp,PID,UID,CPUusr,CPUsys,CPUguest,CPUwait,MinFlt/s,MajFlt/s,VMSZ,VSZ,RSS,Mem%,kB_rd/s,kB_wr/s,Command"}
  $0 ~ /^[0-9]/ {
    ts=strftime("%Y-%m-%dT%H:%M:%S");
    if (NF>=15) {
      print ts,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
    } else { gsub(/[[:space:]]+/, ","); print ts "," $0 }
  }' > "${out_prefix}_pidstat.csv" \
& echo $! > "${out_prefix}_pidstat.pid"


  # iostat per-device extended stats (1s). We turn its blocks into CSV with headers.
  LC_ALL=C iostat -y -dx "$disk" $SAMPLE_INTERVAL \
  | awk -v dev="$disk" -v OFS=',' '
      # 안전하게 값 꺼내는 헬퍼
      function val(idx,    x) {
        if (idx>0 && idx<=NF) return $(idx);
        return 0
      }

      BEGIN {
        printed_header = 0
        have_map = 0
      }

      # 새 섹션 헤더 (장치 표 머리글)
      /^Device/ {
        # 컬럼명 → 인덱스 맵
        delete h
        for (i=1; i<=NF; i++) h[$i]=i
        have_map = 1

        # 버전별 컬럼명 보정
        rk = ( "rkB/s" in h ? h["rkB/s"] : ( "rKB/s" in h ? h["rKB/s"] : 0 ))
        wk = ( "wkB/s" in h ? h["wkB/s"] : ( "wKB/s" in h ? h["wKB/s"] : 0 ))
        util = ( "%util" in h ? h["%util"] : 0 )
        aq = ( "avgqu-sz" in h ? h["avgqu-sz"] : 0 )
        await = ( "await" in h ? h["await"] : 0 )
        rs = ( "r/s" in h ? h["r/s"] : 0 )
        ws = ( "w/s" in h ? h["w/s"] : 0 )

        if (!printed_header) {
          print "timestamp,Device,r/s,w/s,rkB/s,wkB/s,avgqu-sz,await,%util"
          printed_header=1
        }
        next
      }

      # 장치 데이터 라인 (헤더가 맵핑된 뒤에만 처리)
      have_map && $1==dev {
        ts=strftime("%Y-%m-%dT%H:%M:%S")
        print ts,$1,val(rs),val(ws),val(rk),val(wk),val(aq),val(await),val(util)
      }
  ' > "${out_prefix}_iostat.csv" \
  & echo $! > "${out_prefix}_iostat.pid"



  # vmstat system snapshot (1s). First line(s) are headers; convert to CSV with timestamp.
  LC_ALL=C stdbuf -oL vmstat $SAMPLE_INTERVAL \
    | awk -v OFS=',' '
      BEGIN { printed_header = 0 }
      # Match header line like " r  b   swpd  ..."
      /^[[:space:]]*r[[:space:]]+b[[:space:]]+/ {
        if (!printed_header) {
          print "timestamp","r","b","swpd","free","buff","cache","si","so","bi","bo","in","cs","us","sy","id","wa","st"
          printed_header = 1
        }
        next
      }
      # Skip other header lines
      /procs|---/ { next }
      # Process data lines after header is seen
      printed_header && NF >= 17 {
        ts=strftime("%Y-%m-%dT%H:%M:%S");
        print ts,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
      }' > "${out_prefix}_vmstat.csv" \
    & echo $! > "${out_prefix}_vmstat.pid"
}

stop_monitors() {
  local out_prefix="$1"
  for name in pidstat iostat vmstat; do
    local pidfile="${out_prefix}_${name}.pid"
    if [ -f "$pidfile" ]; then
      local mpid
      mpid=$(cat "$pidfile" 2>/dev/null || true)
      if [ -n "$mpid" ] && kill -0 "$mpid" 2>/dev/null; then
        kill "$mpid" 2>/dev/null || true
        # give it a moment to flush
        sleep 0.3
        kill -9 "$mpid" 2>/dev/null || true
      fi
      rm -f "$pidfile"
    fi
  done
}
# ===== end helpers =====
