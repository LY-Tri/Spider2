# activate .venv if available
if [ -f .venv/bin/activate ]; then
    source ../../.venv/bin/activate
fi

source ../../openai.key
export OPENAI_API_KEY
export OPENAI_API_BASE

INPUT_FILE="../../spider2-snow/spider2-snow.jsonl"
SYSTEM_PROMPT="./prompts/spider_agent.txt"
DATABASES_PATH="../../spider2-snow/resource/databases"  # MUST fill your own absolute path
DOCUMENTS_PATH="../../spider2-snow/resource/documents"

MODEL="gpt-5.2"
TEMPERATURE=0.7
TOP_P=0.9
MAX_NEW_TOKENS=12000
MAX_ROUNDS=25
NUM_THREADS=1
ROLLOUT_NUMBER=1
EXPERIMENT_SUFFIX="exp4" 

OUTPUT_FOLDER="./results/${MODEL}_temp${TEMPERATURE}_rounds${MAX_ROUNDS}_rollout${ROLLOUT_NUMBER}_${EXPERIMENT_SUFFIX}"

mkdir -p "./results"

echo "Output file will be: $OUTPUT_FOLDER"



# macOS compatible: use ipconfig for IP, jot for random number
if [[ "$OSTYPE" == "darwin"* ]]; then
    host=$(echo "127.0.0.1")
    port=$(jot -r 1 30000 31000)
else
    host=$(hostname -I | awk '{print $1}')
    port=$(shuf -i 30000-31000 -n 1)
fi
tool_server_url=http://$host:$port/get_observation
python -m servers.serve --workers_per_tool 32 --host $host --port $port  &
server_pid=$!

echo "Server (pid=$server_pid) started at $tool_server_url"

sleep 3

python agent/main.py \
    --input_file "$INPUT_FILE" \
    --output_folder "$OUTPUT_FOLDER" \
    --system_prompt_path "$SYSTEM_PROMPT" \
    --databases_path "$DATABASES_PATH" \
    --documents_path "$DOCUMENTS_PATH" \
    --model "$MODEL" \
    --temperature "$TEMPERATURE" \
    --top_p "$TOP_P" \
    --max_new_tokens "$MAX_NEW_TOKENS" \
    --api_host "$host" \
    --api_port "$port" \
    --max_rounds "$MAX_ROUNDS" \
    --num_threads "$NUM_THREADS" \
    --rollout_number "$ROLLOUT_NUMBER"