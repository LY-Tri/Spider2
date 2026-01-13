import json
import argparse
import random
from pathlib import Path

def extract_sql_answers(input_dir, output_folder):
    """
    Extract SQL answers from terminated records in JSON files
    """
    input_path = Path(input_dir)
    output_path = Path(output_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    
    json_files = list(input_path.glob("*.json"))
    
    processed_count = 0
    skipped_count = []
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                skipped_count.append([json_file.name, "not a list"])
                continue
            
            terminated_records = [record for record in data if record.get('terminated', False)]
            if not terminated_records:
                skipped_count.append([json_file.name, "no terminated records"])
                continue
            
            selected_record = random.choice(terminated_records)
            instance_id = selected_record.get('instance_id') or selected_record.get('id')
            conversation = selected_record.get('conversation', [])

            if not instance_id or not conversation:
                skipped_count.append([json_file.name, "no instance_id or conversation"])
                continue
            
            active_steps = {} # step_name -> sql_block
            refinement_history = [] # list of (sql_block, reason)
            final_answer = None

            for i, item in enumerate(conversation):
                if item.get('role') == 'assistant':
                    tool_calls = item.get('tool_calls', [])
                    for tc in tool_calls:
                        name = tc.get('name')
                        args = tc.get('arguments', {})
                        if isinstance(args, str):
                            try: args = json.loads(args)
                            except: continue
                        
                        if name in ['execute_sql_step', 'execute_snowflake_sql']:
                            sql = args.get('sql')
                            if not sql: continue
                            
                            step_name = args.get('step_name')
                            
                            # Check if this call was successful by looking at the next tool message
                            success = True
                            if i + 1 < len(conversation):
                                next_item = conversation[i+1]
                                if next_item.get('role') == 'tool':
                                    resp = next_item.get('content', '')
                                    if resp.startswith("SQL Error") or resp.startswith("Unexpected error"):
                                        success = False
                            
                            # Mimic the tool's behavior of wrapping SELECTs in TEMP TABLEs
                            if name == 'execute_sql_step' and step_name and sql.strip().upper().startswith("SELECT"):
                                sql = f"CREATE OR REPLACE TEMP TABLE {step_name} AS\n{sql.strip()}"

                            sql_block = f"-- [{name}] {step_name if step_name else 'check'}\n{sql.strip()}"
                            
                            if not success:
                                refinement_history.append((sql_block, "FAILED ATTEMPT"))
                            elif not step_name:
                                refinement_history.append((sql_block, "EXPLORATORY CHECK"))
                            else:
                                if step_name in active_steps:
                                    # Supersede previous successful version of this step
                                    refinement_history.append((active_steps[step_name], "SUPERSEDED VERSION"))
                                active_steps[step_name] = sql_block
                        
                        elif name == 'terminate':
                            answer = args.get('answer')
                            if answer:
                                final_answer = f"-- [final_answer]\n{answer.strip()}"

            # Prepare the final content
            output_lines = []
            
            if refinement_history:
                output_lines.append("-- ========================================================")
                output_lines.append("-- REFINEMENT HISTORY (Exploratory / Failed / Superseded)")
                output_lines.append("-- ========================================================")
                for sql, reason in refinement_history:
                    output_lines.append(f"-- Reason: {reason}")
                    # Comment out the entire block
                    commented_sql = "\n".join([f"-- {line}" for line in sql.split("\n")])
                    output_lines.append(commented_sql)
                    output_lines.append("--;\n")
            
            output_lines.append("-- ========================================================")
            output_lines.append("-- FINAL EXECUTABLE SEQUENCE")
            output_lines.append("-- ========================================================")
            
            # Add active steps in the order they were first seen (to preserve dependencies)
            # We use a trick: dict maintains insertion order since Python 3.7
            for step_name, sql in active_steps.items():
                output_lines.append(sql + ";\n")
            
            if final_answer:
                output_lines.append(final_answer + ";")
            
            if not active_steps and not final_answer:
                skipped_count.append([json_file.name, "no sql found"])
                continue

            # Save to SQL file
            sql_file = output_path / f"{instance_id}.sql"
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(output_lines))
            
            processed_count += 1
            print(f"Extracted cleaned SQL sequence from {json_file.name} -> {instance_id}.sql")
            
        except Exception as e:
            print(f"Error processing {json_file.name}: {e}")
            skipped_count.append([json_file.name, str(e)])
    
    print(f"\nProcessed: {processed_count} files")
    print(f"Skipped: {len(skipped_count)} files")
    with open(output_path / "skipped.log", 'w', encoding='utf-8') as f:
        for item in skipped_count:
            f.write(f"{item[0]}: {item[1]}\n")

def main():
    parser = argparse.ArgumentParser(description='Extract SQL answers from terminated JSON records')
    parser.add_argument('input_dir', help='Input JSON directory path')
    parser.add_argument('output_folder', help='Output folder for SQL files')
    
    args = parser.parse_args()
    
    input_path = Path(args.input_dir)
    if not input_path.exists() or not input_path.is_dir():
        print(f"Error: Invalid input directory: {args.input_dir}")
        return 1
    
    try:
        extract_sql_answers(args.input_dir, args.output_folder)
        return 0
    except Exception as e:
        print(f"Processing error: {e}")
        return 1

if __name__ == '__main__':
    exit(main())