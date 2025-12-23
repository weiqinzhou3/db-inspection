#!/bin/bash
# 多表 MySQL 巡检报告邮件脚本（适配 db-inspection 项目目录）
set -euo pipefail

# projectDir = 当前脚本所在目录的上一级（即仓库根目录）
projectDir=$(
  cd "$(dirname "$0")/.."
  pwd
)

if [ -f "${projectDir}/config/mail_env.sh" ]; then
  # shellcheck disable=SC1091
  . "${projectDir}/config/mail_env.sh"
fi

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

# 所有 TSV 和临时 HTML 都放在这里
OUT_DIR="${projectDir}/out/mysql_analysis"
mkdir -p "${OUT_DIR}"

MAIL_HTML="${OUT_DIR}/mail.html"

# 收件人 / 发件人配置（必须通过环境或 config/mail_env.sh 提供）
EMAIL_RECIVER="${EMAIL_RECIVER:?EMAIL_RECIVER is required (set env or config/mail_env.sh)}"
EMAIL_SENDER="${EMAIL_SENDER:?EMAIL_SENDER is required (set env or config/mail_env.sh)}"
EMAIL_USERNAME="${EMAIL_USERNAME:-${EMAIL_SENDER}}"
EMAIL_PASSWORD="${EMAIL_PASSWORD:?EMAIL_PASSWORD is required (set env or config/mail_env.sh)}"
EMAIL_SMTPHOST="${EMAIL_SMTPHOST:-smtp.qq.com}"
SMTP_SERVER="${EMAIL_SMTPHOST}:587"
EMAIL_TITLE="${EMAIL_TITLE:-[Report] Inspection Server}"

# ========== 1. 配置 5 个表格的描述和对应 TSV 文件路径 ==========

SECTION_TITLES=(
  "1. Q1 - 巡检失败实例明细（最新失败）"
  "2. Q2 - 各环境容量汇总（最新 vs 上一次，仅成功实例）"
  "3. Q3 - 实例容量最近 vs 上一次（含 data/index/total 差异）"
  "4. Q4 - 表维度当前容量 Top10"
  "5. Q5 - 表维度近两次容量差异 Top10"
)

SECTION_FILES=(
  "${OUT_DIR}/q1_failed_instances.tsv"
  "${OUT_DIR}/q2_env_summary.tsv"
  "${OUT_DIR}/q3_instance_last_vs_prev.tsv"
  "${OUT_DIR}/q4_table_last.tsv"
  "${OUT_DIR}/q5_table_diff.tsv"
)

# ========== 2. HTML 生成工具函数 ==========

html_escape() {
  local s="$1"
  s=${s//&/&amp;}
  s=${s//</&lt;}
  s=${s//>/&gt;}
  echo "$s"
}

init_mail_html() {
  cat > "${MAIL_HTML}" <<EOF
<html>
  <body>
    <p>Dear All,</p>
    <br/>
EOF
}

finish_mail_html() {
  cat >> "${MAIL_HTML}" <<EOF
    <br/>
    <p>Best Regards!</p>
  </body>
</html>
EOF
}

# 从一个 TSV 文件生成 HTML 表格
# 规则：
# - 第一行当表头
# - 斑马线底色
# - 所有 *_fmt 列中值为 +xx / -xx 的单元格标红加粗
# - 所有包含 "status" 的列，如果值 != ok，标红加粗
html_table_from_tsv() {
  local tsv_file="$1"

  if [ ! -s "${tsv_file}" ]; then
    echo "<p><i>(no data)</i></p>"
    return
  fi

  echo "<table border=\"2\" style=\"border-collapse:collapse;\" cellpadding=\"6\" cellspacing=\"1\">"

  local line_no=0
  local -a header cols

  while IFS=$'\t' read -r -a cols; do
    if [ $line_no -eq 0 ]; then
      # 表头行
      header=("${cols[@]}")
      echo '<tr style="background-color:#f2f2f2;font-weight:bold;color:#000000;">'
      for col in "${header[@]}"; do
        printf '<th>%s</th>' "$(html_escape "$col")"
      done
      echo "</tr>"
    else
      # 数据行
      if (( line_no % 2 == 0 )); then
        echo '<tr bgcolor="#E6F2FF">'
      else
        echo '<tr>'
      fi

      local idx
      for idx in "${!header[@]}"; do
        local col_name="${header[$idx]}"
        local cell=""
        # 防止 cols 比 header 短时访问越界
        if (( idx < ${#cols[@]} )); then
          cell="${cols[$idx]}"
        else
          cell=""
        fi
        local esc_cell
        esc_cell=$(html_escape "$cell")

        # 异常高亮逻辑：
        # 1) *_fmt 且值以 + / - 开头 → 红+粗
        # 2) 包含 "status" 的列，如果值 != ok → 红+粗
        if [[ "$col_name" == *_fmt ]] && [[ "$cell" == [+-]* ]]; then
          printf '<td><font color="red"><b>%s</b></font></td>' "$esc_cell"
        elif [[ "$col_name" == *status* ]] && [[ -n "$cell" && "$cell" != "ok" ]]; then
          printf '<td><font color="red"><b>%s</b></font></td>' "$esc_cell"
        else
          printf '<td>%s</td>' "$esc_cell"
        fi
      done

      echo "</tr>"
    fi

    line_no=$((line_no + 1))
  done < "${tsv_file}"

  echo "</table>"
}

# 输出一个 section：描述 + 表格
append_section() {
  local desc="$1"
  local tsv_file="$2"

  echo "    <p><b>${desc}</b></p>" >> "${MAIL_HTML}"
  html_table_from_tsv "${tsv_file}" >> "${MAIL_HTML}"
  echo "    <br/>" >> "${MAIL_HTML}"
}

# ========== 3. 发邮件 ==========

send_mail() {
  local EMAIL_CONTENT
  EMAIL_CONTENT=$(cat "${MAIL_HTML}")

  sendEmail \
    -f "$EMAIL_SENDER" \
    -t "$EMAIL_RECIVER" \
    -s "$SMTP_SERVER" \
    -u "$EMAIL_TITLE" \
    -xu "$EMAIL_USERNAME" \
    -xp "$EMAIL_PASSWORD" \
    -m "$EMAIL_CONTENT" \
    -o tls=yes \
    -o message-charset=utf-8 \
    -o message-content-type=html
}

# ========== 4. 主流程 ==========

MAIN() {
  init_mail_html

  local count=${#SECTION_TITLES[@]}
  local i
  for ((i=0; i<${count}; i++)); do
    local title="${SECTION_TITLES[$i]}"
    local file="${SECTION_FILES[$i]}"
    append_section "${title}" "${file}"
  done

  finish_mail_html
  send_mail

  rm -f "${MAIL_HTML}"
}

MAIN
