import ollama
import json
import os
import glob
import argparse

def load_text(filepath: str) -> str:
    """Safely loads text from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"Error: Prompt file not found at '{filepath}'")
        exit() # Exit because the script cannot run without prompts

def find_and_parse_json(response_text: str):
    """
    Robustly finds and parses a JSON object or array from the model's output string.
    """
    # Determine if we're looking for an object or an array
    start_char = '[' if response_text.strip().startswith('[') else '{'
    end_char = ']' if start_char == '[' else '}'
    
    try:
        start_index = response_text.find(start_char)
        end_index = response_text.rfind(end_char)
        
        if start_index == -1 or end_index == -1:
            print("Warning: Could not find start/end of a JSON structure in the response.")
            return None
        
        json_string = response_text[start_index : end_index + 1]
        return json.loads(json_string)
    except Exception as e:
        print(f"JSON parsing failed. Error: {e}")
        print("--- Raw Model Output Snippet ---")
        print(response_text[:500] + "...")
        print("---------------------------------")
        return None

def process_page(page_text: str, prompt_template: str, model_name: str) -> dict | list | None:
    """Sends a single page's content to the LLM with the appropriate prompt."""
    prompt = prompt_template.replace("{{page_text}}", page_text)
    
    print(f"  > Sending page content to model '{model_name}'...")
    try:
        response = ollama.chat(
            model=model_name,
            messages=[{'role': 'user', 'content': prompt}]
        )
        raw_content = response['message']['content']
        print("  > Response received. Parsing JSON...")
        return find_and_parse_json(raw_content)
    except Exception as e:
        print(f"  > An error occurred while communicating with Ollama: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a chunked extraction pipeline on parsed pages.")
    parser.add_argument("input_dir", help="The directory containing the parsed page text files (e.g., 'parsed_statement').")
    parser.add_argument("--model", default="phi3:mini-instruct-4k-q4_0", help="The name of the Ollama model to use.")
    args = parser.parse_args()

    # Load the prompt templates
    prompt_page_1 = load_text("prompt_first_page.txt")
    prompt_next_page = load_text("prompt_next_page.txt")

    # Find and sort the page files to process them in the correct order
    page_files = sorted(glob.glob(os.path.join(args.input_dir, "page_*.txt")))
    
    if not page_files:
        print(f"Error: No page files found in directory '{args.input_dir}'.")
        print("Please run the 'intelligent_page_parser.py' script first.")
        exit()

    final_json_data = {}
    all_transactions = []

    # --- Process Page 1 ---
    if len(page_files) > 0:
        print(f"\n--- Processing Page 1: {os.path.basename(page_files[0])} ---")
        page_1_text = load_text(page_files[0])
        result_page_1 = process_page(page_1_text, prompt_page_1, args.model)
        
        if result_page_1 and isinstance(result_page_1, dict):
            final_json_data = result_page_1
            page_1_transactions = result_page_1.get("transactions", [])
            all_transactions.extend(page_1_transactions)
            final_json_data['transactions'] = all_transactions # Set initial list
            print("  > Page 1 processed successfully.")
        else:
            print("FATAL ERROR: Failed to process Page 1 correctly. The script cannot continue without header information.")
            exit()

    # --- Process Subsequent Pages (Page 2, 3, etc.) ---
    for i in range(1, len(page_files)):
        page_num = i + 1
        print(f"\n--- Processing Page {page_num}: {os.path.basename(page_files[i])} ---")
        page_text = load_text(page_files[i])
        result_page_n = process_page(page_text, prompt_next_page, args.model)
        
        if result_page_n and isinstance(result_page_n, list):
            all_transactions.extend(result_page_n)
            print(f"  > Page {page_num} processed successfully. Found {len(result_page_n)} transactions.")
        else:
            print(f"Warning: Failed to process page {page_num} or received invalid format. Skipping.")
    
    # --- Final Aggregation ---
    final_json_data['transactions'] = all_transactions

    # Save the final, complete JSON file
    output_filename = os.path.basename(os.path.normpath(args.input_dir)) + "_final_structured_data.json"
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(final_json_data, f, indent=4)

    print("\n" + "="*80)
    print("Chunked pipeline completed successfully!")
    print(f"Final aggregated JSON saved to: {output_filename}")
    print("="*80)