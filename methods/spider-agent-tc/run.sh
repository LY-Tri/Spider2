# activate .venv if available
if [ -f .venv/bin/activate ]; then
    source ../../.venv/bin/activate
fi

# Check if Azure OpenAI key file exists, otherwise use standard OpenAI
if [[ -f "../../azure_openai.key" ]]; then
    echo "Using Azure OpenAI configuration..."
    # Load Azure OpenAI credentials directly
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" =~ ^# ]] && continue
        export "$key"="$value"
    done < "../../azure_openai.key"
    echo "  Azure OpenAI credentials loaded"
    echo "  Deployment: $AZURE_OPENAI_DEPLOYMENT"
else
    echo "Using standard OpenAI configuration..."
    if [[ -f "../../openai.key" ]]; then
        source ../../openai.key
        export OPENAI_API_KEY
        export OPENAI_API_BASE
    else
        echo "Error: Neither azure_openai.key nor openai.key found!"
        exit 1
    fi
fi

# Load Snowflake credentials if available
if [[ -f "../../snowflake.key" ]]; then
    echo "Loading Snowflake credentials..."
    # Load Snowflake credentials directly
    while IFS='=' read -r key value; do
        [[ -z "$key" || "$key" =~ ^# ]] && continue
        export "$key"="$value"
    done < "../../snowflake.key"
    echo "  Snowflake credentials loaded for user: $SNOWFLAKE_USER"
else
    echo "Warning: snowflake.key not found - Snowflake queries will fail"
fi

INPUT_FILE="../../spider2-snow/spider2-snow-test10.jsonl"
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
EXPERIMENT_SUFFIX="test10"  # Testing with 10 examples 

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