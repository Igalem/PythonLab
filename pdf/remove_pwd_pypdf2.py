import PyPDF2

def remove_pdf_password(input_pdf_path, output_pdf_path, password):
    try:
        # Open the PDF file
        with open(input_pdf_path, 'rb') as input_file:
            pdf_reader = PyPDF2.PdfReader(input_file)
            
            # Check if the PDF is encrypted
            if pdf_reader.is_encrypted:
                # Attempt to decrypt the PDF
                if pdf_reader.decrypt(password) != 1:
                    print("The password is incorrect.")
                    return
                
                # Create a PDF writer for the output file
                pdf_writer = PyPDF2.PdfWriter()
                
                # Add all pages to the writer
                for page_num in range(len(pdf_reader.pages)):
                    pdf_writer.add_page(pdf_reader.pages[page_num])
                
                # Write to the output PDF file
                with open(output_pdf_path, 'wb') as output_file:
                    pdf_writer.write(output_file)
                
                print(f"Password removed successfully. Saved as {output_pdf_path}")
            else:
                print("PDF is not encrypted.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
input_pdf = "/Users/igale/Downloads/307858308_employeeCard.pdf"
output_pdf = "/Users/igale/Downloads/307858308_newemployeeCard.pdf"
password = "S0BO35"

remove_pdf_password(input_pdf, output_pdf, password)
