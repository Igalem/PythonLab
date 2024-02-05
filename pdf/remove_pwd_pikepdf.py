import pikepdf

def remove_pdf_password_with_pikepdf(input_pdf_path, output_pdf_path, password):
    try:
        # Open the PDF with the provided password
        with pikepdf.open(input_pdf_path, password=password) as pdf:
            # Save the PDF without a password
            pdf.save(output_pdf_path)
        
        print(f"Password removed successfully. Saved as '{output_pdf_path}'")
    except pikepdf.PasswordError:
        print("The password is incorrect.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
input_pdf = "/Users/igale/Downloads/307858308_employeeCard.pdf"
output_pdf = "/Users/igale/Downloads/307858308_newemployeeCard.pdf"
password = "S0BO35"

remove_pdf_password_with_pikepdf(input_pdf, output_pdf, password)
