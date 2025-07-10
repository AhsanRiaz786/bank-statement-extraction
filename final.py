import json
import os
import argparse
import pandas as pd
import tempfile
import glob
from pypdf import PdfReader, PdfWriter
from typing import List, Optional, Dict, Any

# LangChain and Docling Imports
from langchain_docling import DoclingLoader
from langchain_ollama import ChatOllama
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser

# Advanced Docling Configuration Imports
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from langchain_docling.loader import ExportType

def extract_headers_only(pdf_path: str, model_name: str, max_pages_to_scan: int = 3) -> Optional[Dict[str, Any]]:
    """
    Scan the first few pages of the PDF to extract only the column headers/structure.
    Returns the standardized column structure that will be used for all pages.
    """
    print(f"Scanning first {max_pages_to_scan} pages for column headers...")
    
    header_extraction_prompt = PromptTemplate(
        template="""
        You are a bank statement analyzer. Your ONLY job is to identify the column structure of the bank statement table from the provided text.

        Look for:
        1. Table headers/column names
        2. The order of columns (1st, 2nd, 3rd, etc.)
        3. What type of data each column contains

        Common column types in bank statements:
        - Date columns
        - Description/Particulars/Narration columns  
        - Debit/Withdrawal columns
        - Credit/Deposit columns
        - Balance columns
        - Reference/Check number columns

        Output ONLY a JSON object with the column structure. Do NOT extract any transaction data.

        JSON Schema:
        {{
          "column_structure": {{
            "column_order": [
              {{
                "position": 1,
                "header_name": "actual column header name from document",
                "data_type": "date|description|debit|credit|balance|reference|other",
                "standardized_field": "date|description|debit|credit|running_balance|reference|custom_field_name"
              }}
            ],
            "total_columns": "number of columns in the table",
            "table_found": true
          }}
        }}

        Rules:
        1. If no clear table structure is found, set "table_found": false
        2. Use standardized field names: date, description, debit, credit, running_balance, reference
        3. If a column doesn't fit standard types, use a descriptive custom field name
        4. ONLY analyze structure, do NOT extract transaction data
        5. Look for patterns even if headers span multiple lines
        6. ONLY RETURN JSON, NO OTHER TEXT

        Analyze this document text:
        ```markdown
        {document_text}
        ```
        """,
        input_variables=["document_text"],
    )
    
    parser = JsonOutputParser()
    model = ChatOllama(model=model_name, temperature=0.1)
    header_chain = header_extraction_prompt | model | parser
    
    # Try to extract headers from first few pages
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            reader = PdfReader(pdf_path)
            pages_to_scan = min(max_pages_to_scan, len(reader.pages))
            
            for i in range(pages_to_scan):
                print(f"Checking page {i+1} for headers...")
                
                # Create single page PDF
                writer = PdfWriter()
                writer.add_page(reader.pages[i])
                page_pdf_path = os.path.join(temp_dir, f"header_page_{i+1}.pdf")
                with open(page_pdf_path, "wb") as f:
                    writer.write(f)
                
                # Convert to markdown
                pipeline_options = PdfPipelineOptions(do_table_structure=True)
                pipeline_options.table_structure_options.do_cell_matching = True
                pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
                docling_format_options = {InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
                converter = DocumentConverter(format_options=docling_format_options)
                
                loader = DoclingLoader(
                    file_path=page_pdf_path,
                    export_type=ExportType.MARKDOWN,
                    converter=converter
                )
                
                page_docs = loader.load()
                if not page_docs or not page_docs[0].page_content.strip():
                    continue
                    
                markdown_content = page_docs[0].page_content
                
                # Try to extract headers
                try:
                    result = header_chain.invoke({"document_text": markdown_content})
                    
                    if (isinstance(result, dict) and 
                        result.get('column_structure', {}).get('table_found', False) and
                        result.get('column_structure', {}).get('column_order')):
                        
                        # Validate and fix column structure
                        column_structure = result['column_structure']
                        column_order = column_structure.get('column_order', [])
                        
                        # --- START: De-duplicate standardized field names to prevent collisions ---
                        seen_fields = set()
                        for col in column_order:
                            original_field = col.get('standardized_field')
                            
                            if not original_field:
                                continue # Will be handled by the fallback logic later

                            if original_field in seen_fields:
                                # Duplicate found, create a new unique name from the header
                                header_name = col.get('header_name', 'custom_field')
                                new_field = (
                                    header_name.lower()
                                    .replace(' ', '_')
                                    .replace('.', '')
                                    .replace('#', 'no')
                                    .replace('/', '_')
                                    .replace('-', '_')
                                    .replace('(', '')
                                    .replace(')', '')
                                )
                                
                                # Ensure it's truly unique by appending a number if needed
                                counter = 2
                                temp_field = new_field
                                while temp_field in seen_fields:
                                    temp_field = f"{new_field}_{counter}"
                                    counter += 1
                                new_field = temp_field
                                
                                col['standardized_field'] = new_field
                                seen_fields.add(new_field)
                            else:
                                seen_fields.add(original_field)
                        # --- END: De-duplicate standardized field names ---

                        # Fix any missing standardized_field entries
                        for col in column_order:
                            if 'standardized_field' not in col or not col.get('standardized_field'):
                                # Auto-generate standardized field based on data_type
                                data_type = col.get('data_type', 'unknown')
                                if data_type == 'date':
                                    col['standardized_field'] = 'date'
                                elif data_type == 'description':
                                    col['standardized_field'] = 'description'
                                elif data_type == 'debit':
                                    col['standardized_field'] = 'debit'
                                elif data_type == 'credit':
                                    col['standardized_field'] = 'credit'
                                elif data_type == 'balance':
                                    col['standardized_field'] = 'running_balance'
                                elif data_type == 'reference':
                                    col['standardized_field'] = 'reference'
                                else:
                                    # Create a field name from header name
                                    header_name = col.get('header_name', 'unknown')
                                    col['standardized_field'] = header_name.lower().replace(' ', '_').replace('.', '').replace('#', 'no')
                        
                        print(f"✓ Headers found on page {i+1}")
                        print(f"Detected columns: {[col.get('header_name', 'Unknown') for col in column_order]}")
                        return column_structure
                        
                except Exception as e:
                    print(f"Error extracting headers from page {i+1}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error during header extraction: {e}")
            return None
    
    print("No clear table structure found in the first few pages.")
    return None

def create_detailed_transaction_prompt(column_structure: Dict[str, Any]) -> PromptTemplate:
    """
    Create a detailed transaction extraction prompt based on your original specifications.
    """
    column_order = column_structure.get('column_order', [])
    total_columns = column_structure.get('total_columns', 0)
    
    # Build field descriptions
    field_descriptions = []
    example_transaction = {}
    
    for col_info in column_order:
        pos = col_info.get('position', 0)
        header = col_info.get('header_name', 'Unknown')
        field = col_info.get('standardized_field', 'unknown')
        data_type = col_info.get('data_type', 'unknown')
        
        field_descriptions.append(f"- Column {pos} ('{header}') → '{field}' field")
        
        # Build example based on data type
        if data_type == 'date':
            example_transaction[field] = "2024-01-01"
        elif data_type in ['debit', 'credit', 'balance']:
            example_transaction[field] = 1000.50
        else:
            example_transaction[field] = "example_value"
    
    field_desc_text = "\n".join(field_descriptions)
    
    # Create example transaction JSON (with escaped braces)
    example_json_raw = json.dumps(example_transaction, indent=6)
    example_json = example_json_raw.replace("{", "{{").replace("}", "}}")
    
    template = f"""You are a data extraction engine. Analyze the bank statement text provided below. Extract ONLY the transaction line items visible on this page using the standardized column mapping.

COLUMN MAPPING (based on detected structure):
{field_desc_text}

CRITICAL INSTRUCTIONS FOR COLUMN MAPPING:
1. Even if column headers are NOT visible on this page, use the column positions from above.
2. Map data from each column position to the corresponding standardized field.
3. Look for the table structure even if headers are missing - the data should follow the same column order.
4. IMPORTANT: Use column position (1st, 2nd, 3rd, etc.) not header names for mapping.

Instructions:
1. Output a JSON array [...] of transaction objects, no other text or keys.
2. Dates must be in YYYY-MM-DD format.
3. Monetary values must be numbers, remove currency symbols (e.g., "Rs.1,250.50" → 1250.50).
4. Use null for missing values.
5. Do NOT extract numbers from descriptions as debit, credit, or balance.
6. If the page is blank, return an empty array.
7. If the page is not a bank statement, return an empty array.
8. Some transactions can span across multiple rows, so extract all information from the rows.
9. If transaction columns are marked with indicators like withdrawal, deposit, credit, debit then retrieve the information as it is.
10. If debit or credit transactions are present in one column then figure out the transaction type from context.
11. ONLY include fields that actually exist in the document. Do not create placeholder fields.
12. ONLY RETURN THE JSON ARRAY AND NOTHING ELSE. NO EXTRA TEXT OR COMMENTS.
13. Extract ALL transaction rows visible on this page.
14. Multi-row transactions should be combined into single entries.
15. Preserve all original data accuracy, don't infer missing values.
16. Things like "Opening Balance as of 01-JAN-23" should be ignored.
17. Credit and Debit, withdrawal and deposit should always be postive. Don't add any negative sign to them.
EXAMPLE:
```markdown
| Date       | Particulars         | Withdrawal | Deposit | Balance  | Reference |
|------------|---------------------|------------|---------|----------|-----------|
| 01-Jan-2024| SALARY CREDIT       |            | 50000.00| 75000.00 | SAL001    |
| 02-Jan-2024| ATM WITHDRAWAL 1234 | 5000.00    |         | 70000.00 | ATM123    |
```
Output:
[
  {{{{
    "date": "2024-01-01",
    "description": "SALARY CREDIT",
    "debit": null,
    "credit": 50000.00,
    "running_balance": 75000.00,
    "reference": "SAL001"
  }}}},
  {{{{
    "date": "2024-01-02", 
    "description": "ATM WITHDRAWAL 1234",
    "debit": 5000.00,
    "credit": null,
    "running_balance": 70000.00,
    "reference": "ATM123"
  }}}}
]

JSON Example for your document structure:
[
  {example_json}
]

Extract from:
```markdown
{{document_text}}
```"""

    return PromptTemplate(
        template=template,
        input_variables=["document_text"],
    )

def run_improved_docling_pipeline(pdf_path: str, model_name: str, output_path: str):
    """
    Improved pipeline: Extract headers first, then use standardized prompt for all pages.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: Input PDF not found at '{pdf_path}'")
        return

    # Create debug directory
    debug_dir = "debug_logs"
    os.makedirs(debug_dir, exist_ok=True)

    # Step 1: Extract column headers/structure
    print("="*60)
    print("STEP 1: EXTRACTING COLUMN HEADERS")
    print("="*60)
    
    column_structure = extract_headers_only(pdf_path, model_name)
    
    if not column_structure:
        print("❌ Could not detect table structure. Exiting.")
        return
    
    # Save detected structure
    with open(os.path.join(debug_dir, "detected_column_structure.json"), "w") as f:
        json.dump(column_structure, f, indent=2)
    
    print(f"✓ Column structure detected:")
    for col in column_structure.get('column_order', []):
        header_name = col.get('header_name', 'Unknown')
        standardized_field = col.get('standardized_field', 'unknown')
        position = col.get('position', '?')
        print(f"  {position}: {header_name} → {standardized_field}")
    
    # Step 2: Create standardized prompt
    print("\n" + "="*60)
    print("STEP 2: CREATING STANDARDIZED EXTRACTION PROMPT")
    print("="*60)
    
    transaction_prompt = create_detailed_transaction_prompt(column_structure)
    parser = JsonOutputParser()
    model = ChatOllama(model=model_name, temperature=0.1)
    transaction_chain = transaction_prompt | model | parser
    
    # Step 3: Process all pages with the same prompt
    print("\n" + "="*60)
    print("STEP 3: EXTRACTING TRANSACTIONS FROM ALL PAGES")
    print("="*60)
    
    all_transactions = []
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Split PDF into pages
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
        
        # Process each page with the SAME prompt
        for i, page_pdf in enumerate(page_files):
            page_num = i + 1
            print(f"\n--- Processing Page {page_num} of {len(page_files)} ---")
            
            # Convert to markdown
            pipeline_options = PdfPipelineOptions(do_table_structure=True)
            pipeline_options.table_structure_options.do_cell_matching = True
            pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
            docling_format_options = {InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)}
            converter = DocumentConverter(format_options=docling_format_options)
            
            loader = DoclingLoader(
                file_path=page_pdf,
                export_type=ExportType.MARKDOWN,
                converter=converter
            )
            
            try:
                page_docs = loader.load()
                if not page_docs or not page_docs[0].page_content.strip():
                    print(f"Warning: No content extracted from page {page_num}")
                    continue
                    
                markdown_content = page_docs[0].page_content
                
                # Save markdown for debugging
                with open(os.path.join(debug_dir, f"page_{page_num}_markdown.txt"), "w", encoding="utf-8") as f:
                    f.write(markdown_content)
                
                # Extract transactions using the SAME prompt for all pages
                result = transaction_chain.invoke({"document_text": markdown_content})
                
                # Save LLM output
                with open(os.path.join(debug_dir, f"page_{page_num}_transactions.json"), "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2)
                
                if isinstance(result, list):
                    valid_transactions = [tx for tx in result if tx and isinstance(tx, dict)]
                    all_transactions.extend(valid_transactions)
                    print(f"✓ Extracted {len(valid_transactions)} transactions from page {page_num}")
                else:
                    print(f"⚠ Invalid response format from page {page_num}: {type(result)}")
                    
            except Exception as e:
                print(f"Error processing page {page_num}: {e}")
                continue

    # Step 4: Save final results
    print("\n" + "="*60)
    print("STEP 4: SAVING RESULTS")
    print("="*60)
    
    if not all_transactions:
        print("❌ No transactions extracted from any page")
        return
    
    # Add transaction IDs
    for i, tx in enumerate(all_transactions):
        tx['transaction_id'] = i + 1
    
    # Create DataFrame and save
    try:
        df = pd.DataFrame(all_transactions)
        
        # Reorder columns for better readability
        standard_columns = ['transaction_id', 'date', 'description', 'debit', 'credit', 'running_balance', 'reference']
        final_columns = [col for col in standard_columns if col in df.columns]
        final_columns.extend([col for col in df.columns if col not in final_columns])
        
        df = df[final_columns]
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"✅ SUCCESS!")
        print(f"📁 Output saved to: {output_path}")
        print(f"📊 Total transactions: {len(df)}")
        print(f"📋 Columns: {list(df.columns)}")
        print(f"🔍 Debug files saved to: {debug_dir}/")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Improved bank statement extraction with header-first approach")
    parser.add_argument("input_pdf", help="Path to the input PDF file")
    parser.add_argument("--model", default="llama3:8b-instruct", help="Ollama model name")
    parser.add_argument("--output", help="Output CSV file path")
    
    args = parser.parse_args()
    
    if args.output:
        output_path = args.output
    else:
        base_name = os.path.splitext(os.path.basename(args.input_pdf))[0]
        output_path = f"{base_name}_extracted_transactions.csv"
    
    run_improved_docling_pipeline(args.input_pdf, args.model, output_path)