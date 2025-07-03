import json
import os
import argparse
import pandas as pd
import tempfile
import glob
from pypdf import PdfReader, PdfWriter
from typing import List, Optional, Dict, Any

# LangChain Imports
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser

# Unstructured PDF Processing
from unstructured.partition.pdf import partition_pdf

def run_unstructured_pipeline(pdf_path: str, model_name: str, output_path: str):
    """
    Splits a PDF into individual pages, processes each page with unstructured and an LLM,
    extracts only transactions, and saves them to a single CSV file.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: Input PDF not found at '{pdf_path}'")
        return

    # Create a debug directory for logging
    debug_dir = "debug_logs"
    os.makedirs(debug_dir, exist_ok=True)

    # --- 1. Prompts and LLM Setup ---
    first_page_prompt = PromptTemplate(
        template="""
        You are a data extraction engine. Analyze the bank statement HTML table data from PAGE 1 provided below. Extract ONLY the transaction line items visible on this page AND identify the column headers with their positions.

        Instructions:
        1. Output a JSON object with "transactions" array and "column_structure" object.
        2. Dates must be in YYYY-MM-DD format.
        3. Monetary values must be numbers, remove currency symbols (e.g., "Rs.1,250.50" → 1250.50).
        4. Use null for missing values.
        5. Do NOT extract numbers from descriptions as debit, credit, or balance.
        6. If the page is blank, return empty arrays.
        7. If the page is not a bank statement, return empty arrays.
        8. Some transactions can span across multiple rows, so extract all information from the rows.
        9. If transaction columns are marked with indicators like withdrawal, deposit, credit, debit then retrieve the information as it is.
        10. If debit or credit transactions are present in one column then figure out the transaction type from context.
        11. Extract ALL column headers present in the first page, including their exact positions (1st column, 2nd column, etc.).
        12. Map the actual column names to standardized field names in the transactions.
        13. CRITICAL: Record the exact order/position of columns as they appear in the table.
        14. CRITICAL: For transactions, include ALL fields that exist in the document, not just the standard ones.
        15. CRITICAL: Each transaction object should have a field for EVERY column detected in the table.
        16. Use descriptive field names for non-standard columns (e.g., "cheque_number", "branch_code", "transaction_type").
        17. ONLY RETURN THE JSON AND NOTHING ELSE. NO EXTRA TEXT OR COMMENTS.

        JSON Schema (adapt based on actual columns found):
        {{
          "transactions": [
            {{
              // Include ALL fields corresponding to ALL columns found in the table
              // Standard fields:
              "date": "string (YYYY-MM-DD) | null",
              "description": "string | null", 
              "debit": "number | null",
              "credit": "number | null",
              "running_balance": "number | null",
              "reference": "string | null",
              // Additional fields based on actual columns found:
              "cheque_number": "string | null",
              "branch_code": "string | null",
              "transaction_type": "string | null"
              // Add more fields as needed based on the actual table structure
            }}
          ],
          "column_structure": {{
            "column_order": [
              {{
                "position": 1,
                "header_name": "actual column header name",
                "data_type": "date|description|debit|credit|balance|reference|other",
                "standardized_field": "date|description|debit|credit|running_balance|reference|custom_field_name"
              }}
            ],
            "total_columns": "number of columns in the table"
          }}
        }}

        IMPORTANT: 
        - For transactions, create a field for EVERY column in the table
        - Use descriptive names for non-standard columns
        - Every transaction should have the same set of fields (use null for missing values)
        - The transaction fields should match exactly with the standardized_field names in column_structure

        EXAMPLE HTML:
        <table>
        <tr><th>Date</th><th>Particulars</th><th>Cheque No</th><th>Withdrawal</th><th>Deposit</th><th>Balance</th><th>Branch</th><th>Reference</th></tr>
        <tr><td>01-Jan-2024</td><td>SALARY CREDIT</td><td></td><td></td><td>50000.00</td><td>75000.00</td><td>001</td><td>SAL001</td></tr>
        <tr><td>02-Jan-2024</td><td>ATM WITHDRAWAL 1234</td><td></td><td>5000.00</td><td></td><td>70000.00</td><td>001</td><td>ATM123</td></tr>
        </table>

        Output:
        {{
          "transactions": [
            {{
              "date": "2024-01-01",
              "description": "SALARY CREDIT",
              "cheque_number": null,
              "debit": null,
              "credit": 50000.00,
              "running_balance": 75000.00,
              "branch_code": "001",
              "reference": "SAL001"
            }},
            {{
              "date": "2024-01-02", 
              "description": "ATM WITHDRAWAL 1234",
              "cheque_number": null,
              "debit": 5000.00,
              "credit": null,
              "running_balance": 70000.00,
              "branch_code": "001",
              "reference": "ATM123"
            }}
          ],
          "column_structure": {{
            "column_order": [
              {{
                "position": 1,
                "header_name": "Date",
                "data_type": "date",
                "standardized_field": "date"
              }},
              {{
                "position": 2,
                "header_name": "Particulars",
                "data_type": "description",
                "standardized_field": "description"
              }},
              {{
                "position": 3,
                "header_name": "Cheque No",
                "data_type": "other",
                "standardized_field": "cheque_number"
              }},
              {{
                "position": 4,
                "header_name": "Withdrawal",
                "data_type": "debit",
                "standardized_field": "debit"
              }},
              {{
                "position": 5,
                "header_name": "Deposit",
                "data_type": "credit",
                "standardized_field": "credit"
              }},
              {{
                "position": 6,
                "header_name": "Balance",
                "data_type": "balance",
                "standardized_field": "running_balance"
              }},
              {{
                "position": 7,
                "header_name": "Branch",
                "data_type": "other",
                "standardized_field": "branch_code"
              }},
              {{
                "position": 8,
                "header_name": "Reference",
                "data_type": "reference",
                "standardized_field": "reference"
              }}
            ],
            "total_columns": 8
          }}
        }}

        Extract from HTML table data:
        {document_text}
        """,
        input_variables=["document_text"],
    )

    def create_next_page_prompt(column_structure: Dict[str, Any]) -> PromptTemplate:
        """Create a dynamic prompt for subsequent pages based on first page column structure."""
        
        column_order = column_structure.get('column_order', [])
        total_columns = column_structure.get('total_columns', 0)
        
        # Build detailed column mapping information
        column_mapping_info = "Based on the first page analysis, the table structure is:\n"
        column_mapping_info += f"        - Total columns: {total_columns}\n"
        
        for col_info in column_order:
            pos = col_info.get('position', 0)
            header = col_info.get('header_name', 'Unknown')
            data_type = col_info.get('data_type', 'unknown')
            field = col_info.get('standardized_field', 'unknown')
            column_mapping_info += f"        - Column {pos}: '{header}' → {data_type} data → '{field}' field\n"
        
        # Build explicit column mapping for the prompt
        explicit_mapping = "\n"
        for i, col_info in enumerate(column_order, 1):
            field = col_info.get('standardized_field', 'unknown')
            header = col_info.get('header_name', 'Unknown')
            explicit_mapping += f"               Column {i} → {field} (originally '{header}')\n"
        
        # Build the JSON schema as a simple example
        json_example_fields = []
        for col_info in column_order:
            field = col_info.get('standardized_field', 'unknown')
            data_type = col_info.get('data_type', 'unknown')
            
            if data_type == 'date':
                json_example_fields.append(f'      "{field}": "2024-01-01"')
            elif data_type in ['debit', 'credit', 'balance']:
                json_example_fields.append(f'      "{field}": 1000.50')
            else:
                json_example_fields.append(f'      "{field}": "example_value"')
        
        json_example = "{\n" + ",\n".join(json_example_fields) + "\n    }" if json_example_fields else '{\n      "date": "2024-01-01",\n      "description": "example",\n      "debit": 1000.50,\n      "credit": null,\n      "running_balance": 5000.00\n    }'

        # Escape braces to prevent LangChain template variable issues
        escaped_json_example = json_example.replace("{", "{{").replace("}", "}}")

        # Create the template string using the *escaped* JSON example
        template_str = f"""You are a data extraction engine. Analyze the bank statement HTML table data from a subsequent page provided below. Extract ONLY the transaction line items visible on this page.

{column_mapping_info}

CRITICAL INSTRUCTIONS FOR COLUMN MAPPING:
1. Even if column headers are NOT visible on this page, use the column positions from above.
2. Map data from each column position to the corresponding standardized field.
3. If you see a table with data rows, map them in this exact order:{explicit_mapping}
4. Look for the table structure even if headers are missing - the data should follow the same column order as page 1.
5. IMPORTANT: Use column position (1st, 2nd, 3rd, etc.) not header names for mapping.
6. CRITICAL: Each transaction must have ALL the fields defined in the column structure (use null for missing values).

General Instructions:
1. Output a single JSON array [...] of transaction objects, no other text or keys.
2. Dates must be in YYYY-MM-DD format.
3. Monetary values must be numbers, remove currency symbols (e.g., "Rs.1,250.50" → 1250.50).
4. Use null for missing values.
5. Do NOT extract numbers from descriptions as debit, credit, or balance.
6. If the page is blank, return an empty array.
7. If the page is not a bank statement, return an empty array.
8. Some transactions can span across multiple rows, so extract all information from the rows.
9. If debit or credit transactions are present in one column then figure out the transaction type from context.
10. Only include fields that were identified in the first page structure.
11. EVERY transaction object must have the SAME set of fields as defined in the column structure.
12. CRITICAL: ONLY RETURN THE JSON AND NOTHING ELSE. NO EXTRA TEXT OR COMMENTS.

JSON Example (use null for missing values):
[
  {escaped_json_example}
]

Extract from HTML table data:
{{document_text}}"""

        return PromptTemplate(
            template=template_str,
            input_variables=["document_text"],
        )
    
    parser = JsonOutputParser()
    model = ChatOllama(model=model_name, temperature=0.1)
    first_page_chain = first_page_prompt | model | parser
    
    all_transactions = []
    column_structure = None

    with tempfile.TemporaryDirectory() as temp_dir:
        # --- 2. Split PDF into Single Pages ---
        print(f"Splitting PDF into pages inside temporary directory: {temp_dir}")
        try:
            reader = PdfReader(pdf_path)
            for i, page in enumerate(reader.pages):
                writer = PdfWriter()
                writer.add_page(page)
                page_pdf_path = os.path.join(temp_dir, f"page_{i+1:03}.pdf")
                with open(page_pdf_path, "wb") as f:
                    writer.write(f)
        except Exception as e:
            print(f"Error splitting PDF: {e}")
            return
            
        page_files = sorted(glob.glob(os.path.join(temp_dir, "*.pdf")))
        if not page_files:
            print("Could not split PDF into pages.")
            return

        # --- 3. Process Each Page ---
        for i, page_pdf in enumerate(page_files):
            page_num = i + 1
            print(f"\n--- Processing Page {page_num} of {len(page_files)} ---")
            
            # --- 3a. Extract table data using unstructured partition_pdf ---
            try:
                print(f"Running unstructured partition_pdf on page {page_num}...")
                elements = partition_pdf(
                    filename=page_pdf,
                    extract_images_in_pdf=False,
                    strategy="hi_res",
                    infer_table_structure=True,
                    hi_res_model_name="detectron2_onnx"
                )
                
                # Filter for table elements and get HTML
                tables = [e for e in elements if e.category == "Table"]
                
                if not tables:
                    print(f"Warning: No tables found on page {page_num}.")
                    continue
                
                # Combine all table HTML from the page
                table_html_content = ""
                for j, table in enumerate(tables):
                    if hasattr(table, 'metadata') and hasattr(table.metadata, 'text_as_html'):
                        table_html_content += f"\n<!-- Table {j+1} -->\n{table.metadata.text_as_html}\n"
                    else:
                        # Fallback to text content if HTML not available
                        table_html_content += f"\n<!-- Table {j+1} (text fallback) -->\n{str(table)}\n"
                
                if not table_html_content.strip():
                    print(f"Warning: No table content extracted from page {page_num}.")
                    continue
                
                # Save HTML content to debug file
                with open(os.path.join(debug_dir, f"page_{page_num}_html.txt"), "w", encoding="utf-8") as f:
                    f.write(table_html_content)
                    
            except Exception as e:
                print(f"Error running unstructured on page {page_num}: {e}")
                continue

            # --- 3b. Extract transactions using the appropriate chain ---
            extracted_data = None
            try:
                print(f"Sending page {page_num} to '{model_name}' for extraction...")
                
                if page_num == 1:
                    # First page: extract both transactions and column structure
                    result = first_page_chain.invoke({"document_text": table_html_content})
                    extracted_data = result
                    
                    # Save raw LLM output
                    with open(os.path.join(debug_dir, f"page_{page_num}_llm_output.json"), "w", encoding="utf-8") as f:
                        json.dump(result, f, indent=2)
                    
                    if not isinstance(result, dict) or 'transactions' not in result:
                        print(f"Error: First page LLM output is not in expected format: {result}")
                        continue
                        
                    # Extract column structure for use in subsequent pages
                    column_structure = result.get('column_structure', {})
                    print(f"Extracted column structure: {json.dumps(column_structure)}")
                    
                    # Log the column mapping details
                    if column_structure and column_structure.get('column_order'):
                        print("Column mapping detected:")
                        for col_info in column_structure['column_order']:
                            pos = col_info.get('position', '?')
                            header = col_info.get('header_name', 'Unknown')
                            field = col_info.get('standardized_field', 'unknown')
                            print(f"  Position {pos}: '{header}' → '{field}'")
                    
                    # Save column structure to debug file
                    with open(os.path.join(debug_dir, "extracted_column_structure.json"), "w", encoding="utf-8") as f:
                        json.dump(column_structure, f, indent=2)
                    
                    transactions = result.get('transactions', [])
                    if transactions:
                        all_transactions.extend(transactions)
                        
                else:
                    # Subsequent pages: use dynamic prompt based on first page structure
                    if column_structure is None or not column_structure.get('column_order'):
                        print(f"Warning: No column structure available from first page, skipping page {page_num}. First page must contain the table structure.")
                        continue
                    
                    next_page_prompt = create_next_page_prompt(column_structure)
                    next_page_chain = next_page_prompt | model | parser
                    
                    print(f"Using column structure for page {page_num}:")
                    if column_structure and column_structure.get('column_order'):
                        for col_info in column_structure['column_order']:
                            pos = col_info.get('position', '?')
                            field = col_info.get('standardized_field', 'unknown')
                            print(f"  Column {pos} → '{field}'")
                    
                    result = next_page_chain.invoke({"document_text": table_html_content})
                    extracted_data = result
                    
                    # Save raw LLM output
                    with open(os.path.join(debug_dir, f"page_{page_num}_llm_output.json"), "w", encoding="utf-8") as f:
                        json.dump(result, f, indent=2)
                    
                    if not isinstance(result, list):
                        print(f"Error: LLM output for page {page_num} is not a valid list: {result}")
                        continue
                        
                    if result:
                        all_transactions.extend(result)
                
                # Unified success message
                num_extracted = 0
                if isinstance(extracted_data, dict):
                    num_extracted = len(extracted_data.get('transactions', []))
                elif isinstance(extracted_data, list):
                    num_extracted = len(extracted_data)
                print(f"Extracted {num_extracted} transactions from page {page_num}")
                        
            except json.JSONDecodeError as jde:
                print(f"JSON validation error for page {page_num}: {jde}")
                print(f"Raw output saved to: {os.path.join(debug_dir, f'page_{page_num}_llm_output.json')}")
                continue
            except Exception as e:
                print(f"An error occurred during LLM extraction for page {page_num}: {e}")
                continue

    # --- 4. Aggregate Results and Save to CSV ---
    if not all_transactions:
        print("\nNo transactions were extracted from the document.")
        return

    for i, tx in enumerate(all_transactions):
        if tx: # Ensure transaction is not None or empty
            tx['transaction_id'] = i + 1

    try:
        # Filter out any potential None values that may have slipped in
        df = pd.DataFrame([tx for tx in all_transactions if tx])
        
        # Ensure standard columns are present and in preferred order
        standard_columns = ['transaction_id', 'date', 'description', 'debit', 'credit', 'running_balance', 'reference']
        final_columns = []
        
        # Add standard columns that exist in the dataframe
        for col in standard_columns:
            if col in df.columns:
                final_columns.append(col)
        
        # Add any additional (custom) columns that aren't in the standard set
        for col in df.columns:
            if col not in final_columns:
                final_columns.append(col)
                
        df = df[final_columns]
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print("\n" + "="*80)
        print("Unstructured pipeline completed successfully!")
        print(f"Final aggregated CSV saved to: {output_path}")
        print(f"Total transactions extracted: {len(df)}")
        print(f"Columns in final output: {list(df.columns)}")
        if column_structure:
            print(f"Column structure detected: {json.dumps(column_structure.get('column_order'))}")
        print("="*80)

    except Exception as e:
        print(f"An error occurred while creating or saving the CSV file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full unstructured+LangChain extraction pipeline.")
    parser.add_argument("input_pdf", help="The path to the input PDF file.")
    parser.add_argument("--model", default="llama3:8b-instruct", help="The name of the Ollama model to use.")
    args = parser.parse_args()

    base_name = os.path.splitext(os.path.basename(args.input_pdf))[0]
    output_filename = f"{base_name}_final_data.csv"

    run_unstructured_pipeline(pdf_path=args.input_pdf, model_name=args.model, output_path=output_filename)