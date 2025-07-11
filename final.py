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

def clean_monetary_value(value):
    """Clean monetary values to ensure they are pure numbers."""
    if value is None or value == "":
        return None
    
    # Convert to string if it's not already
    str_value = str(value).strip()
    
    if not str_value or str_value.lower() in ['null', 'none', 'n/a', '-']:
        return None
    
    # Remove common monetary suffixes and prefixes
    # Remove Cr, Dr, Credit, Debit (case insensitive)
    import re
    str_value = re.sub(r'\b(cr|dr|credit|debit)\b', '', str_value, flags=re.IGNORECASE)
    
    # Remove currency symbols and common prefixes
    str_value = re.sub(r'[₹$£€¥₹Rs\.USD\s]', '', str_value)
    
    # Remove commas
    str_value = str_value.replace(',', '')
    
    # Remove any remaining non-digit characters except decimal point and minus
    str_value = re.sub(r'[^\d\.\-]', '', str_value)
    
    # Handle empty string after cleaning
    if not str_value:
        return None
    
    try:
        # Convert to float and take absolute value (monetary amounts should always be positive)
        # Debit means "money out" but the amount itself should be positive
        # Credit means "money in" and the amount should be positive
        return abs(float(str_value))
    except (ValueError, TypeError):
        return None

def parse_with_retry(chain, input_data: dict, max_retries: int = 2):
    """Parse with retry logic for malformed JSON responses."""
    for attempt in range(max_retries + 1):
        try:
            result = chain.invoke(input_data)
            return result
        except Exception as e:
            if attempt < max_retries:
                print(f"  ⚠ Parse attempt {attempt + 1} failed: {e}. Retrying...")
                continue
            else:
                print(f"  ❌ All parse attempts failed. Last error: {e}")
                raise e

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
                    result = parse_with_retry(header_chain, {"document_text": markdown_content})
                    
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

def create_detailed_transaction_prompt(
    column_structure: Dict[str, Any], 
    last_transaction: Optional[Dict[str, Any]] = None
) -> PromptTemplate:
    """
    Create a detailed transaction extraction prompt based on your original specifications.
    An optional last_transaction can be provided for context.
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
    
    # --- START: Add context from last transaction ---
    context_section = ""
    if last_transaction:
        # Sanitize for prompt injection
        safe_last_tx = {k: v for k, v in last_transaction.items() if isinstance(v, (str, int, float, bool) or v is None)}
        last_tx_json = json.dumps(safe_last_tx, indent=2)
        # Escape all curly braces for LangChain template
        escaped_json = last_tx_json.replace("{", "{{").replace("}", "}}")
        
        context_section = f"""
CONTEXT FROM PREVIOUS PAGE:
- The previous page's last transaction was: {escaped_json}
- Ensure all fields in the new transactions you extract follow the SAME data format.
- For example, if 'running_balance' was a number, it must remain a number. If a date was 'YYYY-MM-DD', new dates must also be in that format.
- Apply this formatting logic to ALL columns to maintain consistency.
"""
    # --- END: Add context from last transaction ---

    template = f"""You are a precise bank statement data extraction engine. Your PRIMARY GOAL is to extract EVERY SINGLE transaction that has a date - NEVER skip any transaction row.

COLUMN MAPPING (based on detected structure):
{field_desc_text}

CRITICAL EXTRACTION RULES:
1. EXTRACT EVERY ROW that contains a date - this is MANDATORY
2. Even if column headers are NOT visible on this page, use the column positions from above
3. Map data from each column position to the corresponding standardized field
4. Look for the table structure even if headers are missing - data follows the same column order
5. Use column position (1st, 2nd, 3rd, etc.) not header names for mapping
{context_section}

MANDATORY EXTRACTION INSTRUCTIONS:
1. Scan the ENTIRE page systematically from top to bottom
2. Look for ANY row that contains a date (in ANY format: DD-MM-YYYY, MM/DD/YYYY, DD/MM/YY, etc.)
3. If you find a date, that row MUST be extracted as a transaction - NO EXCEPTIONS
4. Some transactions may span multiple rows - combine them into single entries
5. Look for dates even in merged cells, partial tables, or broken table structures
6. Extract transactions even if some columns are empty or missing
7. DO NOT skip transactions because of formatting issues or unclear data

DATA FORMATTING RULES:
1. Output a JSON array [...] of transaction objects, no other text or keys
2. Convert ALL dates to YYYY-MM-DD format (parse flexibly: 01-Jan-2024 → 2024-01-01)
3. MONETARY VALUES MUST BE PURE NUMBERS ONLY:
   - Remove ALL text suffixes: "1,250.50 Cr" → 1250.50
   - Remove ALL currency symbols: "Rs.1,250.50" → 1250.50  
   - Remove ALL prefixes/suffixes: "Dr 500.00", "500.00 Dr", "500.00 Cr" → 500.00
   - Remove commas from numbers: "1,250.50" → 1250.50
   - Convert to decimal numbers: 1250.50 (NOT strings)
4. Use null for missing values, but still include the transaction
5. Do NOT extract numbers from descriptions as debit, credit, or balance
6. CRITICAL: All monetary amounts must be POSITIVE numbers:
   - Debit amounts should be positive (e.g., 500.00, NOT -500.00)
   - Credit amounts should be positive (e.g., 1000.00, NOT -1000.00) 
   - Debit already means "money out", so don't make it negative
   - Credit already means "money in", so keep it positive
7. Preserve all original description text exactly as written

CRITICAL MONETARY CLEANING EXAMPLES:
- "1,142,432.00Cr" → 1142432.00 (credit amount, positive)
- "Rs.50,000.00" → 50000.00 (remove currency symbol)
- "5000 Dr" → 5000.00 (debit amount, positive - NOT negative)
- "2,500.75 Cr" → 2500.75 (credit amount, positive)
- "$1,000.00" → 1000.00 (remove currency symbol)
- "-500.00 Dr" → 500.00 (debit should be positive, remove minus sign)

WHAT TO IGNORE (but still check for dates):
- Page headers/footers without dates
- Opening/closing balance statements WITHOUT transaction dates
- Summary rows without specific transaction dates
- Account information sections

WHAT TO ALWAYS EXTRACT:
- ANY row with a transaction date
- Transaction descriptions (even if incomplete)
- Any monetary amounts in the correct columns
- Reference numbers or transaction IDs
- Running balances when available

CRITICAL: If the page is completely blank or contains no dates whatsoever, return an empty array. Otherwise, you MUST extract every single row that contains a date.

VERIFICATION CHECKLIST before outputting:
☐ Did I scan the ENTIRE page for dates?
☐ Did I extract EVERY row that contains a date?
☐ Did I check for multi-row transactions?
☐ Did I look in broken or partial tables?
☐ Are all dates in YYYY-MM-DD format?
☐ Are all monetary values positive numbers?

ONLY RETURN THE JSON ARRAY AND NOTHING ELSE. NO EXTRA TEXT OR COMMENTS.
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
        
    # Identify debit/credit fields that should always be positive
    positive_fields = []
    for col in column_structure.get('column_order', []):
        if col.get('data_type') in ['debit', 'credit']:
            if standardized_field := col.get('standardized_field'):
                positive_fields.append(standardized_field)
    
    if positive_fields:
        print(f"ⓘ Forcing positive values for: {positive_fields}")
        
    # Identify date fields for consistent formatting
    date_fields = []
    for col in column_structure.get('column_order', []):
        if col.get('data_type') == 'date':
            if standardized_field := col.get('standardized_field'):
                date_fields.append(standardized_field)

    if date_fields:
        print(f"ⓘ Standardizing date format for: {date_fields}")
    
    # Save detected structure
    with open(os.path.join(debug_dir, "detected_column_structure.json"), "w") as f:
        json.dump(column_structure, f, indent=2)
    
    print(f"✓ Column structure detected:")
    for col in column_structure.get('column_order', []):
        header_name = col.get('header_name', 'Unknown')
        standardized_field = col.get('standardized_field', 'unknown')
        position = col.get('position', '?')
        print(f"  {position}: {header_name} → {standardized_field}")
    
    # Step 2: Set up model and parser
    print("\n" + "="*60)
    print("STEP 2: PREPARING EXTRACTION MODEL")
    print("="*60)
    
    # Initialize these once
    parser = JsonOutputParser()
    model = ChatOllama(model=model_name, temperature=0.1)
    print("✓ Model and parser are ready.")

    # Step 3: Process all pages with the same prompt
    print("\n" + "="*60)
    print("STEP 3: EXTRACTING TRANSACTIONS FROM ALL PAGES")
    print("="*60)
    
    all_transactions = []
    last_successful_transaction = None # To provide context to the next page
    
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
        
        # Process each page, now with context
        for i, page_pdf in enumerate(page_files):
            page_num = i + 1
            print(f"\n--- Processing Page {page_num} of {len(page_files)} ---")
            
            # Create a new prompt for each page, potentially with context
            transaction_prompt = create_detailed_transaction_prompt(
                column_structure,
                last_transaction=last_successful_transaction
            )
            transaction_chain = transaction_prompt | model | parser
            
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
                
                # Extract transactions using the context-aware prompt
                result = parse_with_retry(transaction_chain, {"document_text": markdown_content})
                
                # Save LLM output
                with open(os.path.join(debug_dir, f"page_{page_num}_transactions.json"), "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2)
                
                if isinstance(result, list):
                    valid_transactions = [tx for tx in result if tx and isinstance(tx, dict)]
                    
                    # Post-process to clean all monetary fields
                    for tx in valid_transactions:
                        for col_info in column_structure.get('column_order', []):
                            field = col_info.get('standardized_field')
                            data_type = col_info.get('data_type')
                            
                            # Clean monetary fields (debit, credit, balance)
                            if data_type in ['debit', 'credit', 'balance'] and field in tx:
                                tx[field] = clean_monetary_value(tx[field])
                    
                    # Additional cleanup for positive_fields (legacy support)
                    if positive_fields:
                        for tx in valid_transactions:
                            for field in positive_fields:
                                if field in tx and tx[field] is not None:
                                    tx[field] = clean_monetary_value(tx[field])
                                        
                    # Post-process to standardize date formats
                    if date_fields:
                        for tx in valid_transactions:
                            for field in date_fields:
                                if field in tx and tx[field]:
                                    try:
                                        # Use pandas to flexibly parse the date and format it
                                        standardized_date = pd.to_datetime(tx[field], errors='coerce')
                                        if pd.notna(standardized_date):
                                            tx[field] = standardized_date.strftime('%Y-%m-%d')
                                    except Exception:
                                        # In case of any other parsing error, keep original
                                        pass
                                        
                    if valid_transactions:
                        all_transactions.extend(valid_transactions)
                        last_successful_transaction = valid_transactions[-1] # Update context
                        print(f"✓ Extracted {len(valid_transactions)} transactions from page {page_num}")
                    else:
                        print(f"ⓘ No transactions found on page {page_num}")
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
    parser = argparse.ArgumentParser(description="Improved bank statement extraction with header-first and context-aware approach")
    parser.add_argument("input_pdf", help="Path to the input PDF file")
    parser.add_argument("--model", default="llama3.1:8b", help="Ollama model name")
    parser.add_argument("--output", help="Output CSV file path")
    
    args = parser.parse_args()
    
    if args.output:
        output_path = args.output
    else:
        base_name = os.path.splitext(os.path.basename(args.input_pdf))[0]
        output_path = f"{base_name}_extracted_transactions.csv"
    
    run_improved_docling_pipeline(args.input_pdf, args.model, output_path)