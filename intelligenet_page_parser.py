import os
from unstructured.partition.pdf import partition_pdf
import argparse

def parse_pdf_by_page(pdf_path: str, output_dir: str):
    """
    Parses a PDF using 'unstructured' and saves the content of each page
    into a separate text file.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: Input PDF not found at '{pdf_path}'")
        return

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting page-by-page parsing for: {pdf_path}...")
    try:
        # The partition_pdf function returns elements with metadata, including page number
        elements = partition_pdf(filename=pdf_path, strategy="hi_res", languages=['eng'])
        
        page_content = {}
        for el in elements:
            page_num = el.metadata.page_number
            if page_num not in page_content:
                page_content[page_num] = []
            page_content[page_num].append(el.text)
            
        # Write content for each page to a separate file
        for page_num, texts in page_content.items():
            output_filepath = os.path.join(output_dir, f"page_{page_num}.txt")
            with open(output_filepath, "w", encoding="utf-8") as f:
                f.write("\n\n".join(texts))
            print(f"Saved page {page_num} to {output_filepath}")

        print("\nPage-by-page parsing completed successfully.")

    except Exception as e:
        print(f"An error occurred during parsing: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse a PDF page by page.")
    parser.add_argument("input_pdf", help="The path to the input PDF file.")
    parser.add_argument("output_dir", help="The directory to save the page text files.")
    args = parser.parse_args()
    
    parse_pdf_by_page(pdf_path=args.input_pdf, output_dir=args.output_dir)