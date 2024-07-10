from PyPDF2 import PdfReader, PdfWriter
import cups
import os


def print_document(file_path, printer_name, quantity=1, double_sided=False, color=False):
    conn = cups.Connection()
    
    # check if the printer exists
    printers = conn.getPrinters()
    if printer_name not in printers:
        print(f"Printer {printer_name} not found")
        return

    # prepare print options
    options = {
        "copies": str(quantity),
        "sides": "two-sided-long-edge" if double_sided else "one-sided",
        "ColorModel": "Color" if color else "Grayscale"
    }

    # submit the print job
    try:
        job_id = conn.printFile(printer_name, file_path, "Print Job", options)
        print(f"Print job submitted. JOB ID: {job_id}")
    except cups.IPPError as e:
        print(f"Error submitting print job: {e}")


def rotate_pdf(input_path, output_path, rotation):
    with open(input_path, 'rb') as file:
        reader = PdfReader(file)
        writer = PdfWriter()

        for page in reader.pages:
            page.rotate(rotation)
            writer.add_page(page)

        with open(output_path, 'wb') as output_file:
            writer.write(output_file)


def main():
    printer_name = 'HP_LaserJet_Professional_M1136_MFP'

    # example usage
    document_path = 'ToBePrinted/info-1.pdf'
    quantity = 1
    double_sided = False
    color = False
    position = 'portrait'

    if position == 'landscape':
        rotated_path = document_path.replace('.pdf', '_rotated.pdf')
        rotate_pdf(document_path, rotated_path, 90)

    print_document(document_path, printer_name, quantity, double_sided, color)

    if position == 'landscape':
        os.remove(rotated_path)


if __name__ == '__main__':
    main()
