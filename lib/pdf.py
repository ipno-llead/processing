from typing import Union
from PyPDF2 import PdfFileReader, PdfFileWriter


class PDFSubsetError(ValueError):
    pass


def subset_pdf(
    input_filepath: str,
    output_filepath: str,
    page_from: int,
    page_to: Union[int, None] = None,
):
    """Subsets a PDF file

    Args:
        input_filepath (str):
            file path of input PDF file
        output_filepath (str):
            file path of output PDF file
        page_from (int):
            start page (inclusive)
        page_to (int | None):
            end page (exclusive). If not given, assumed to
            be page_from + 1.

    Returns:
        nothing
    """
    if page_to is None:
        page_to = page_from + 1
    if page_from >= page_to:
        raise PDFSubsetError("page_from must be less than page_to")
    with open(input_filepath, "rb") as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        if page_to > total:
            raise PDFSubsetError(
                "error subsetting %s: page_to (%d) must be less than or equal to total (%d)"
                % (input_filepath, page_to, total)
            )
        writer = PdfFileWriter()
        for page in range(page_from, page_to):
            writer.addPage(reader.getPage(page))
        with open(output_filepath, "wb") as wf:
            writer.write(wf)
