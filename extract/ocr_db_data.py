import sys

sys.path.append("../")
from wand.image import Image
from PIL import Image as PI
import io
import os
import pyocr
import pyocr.builders
import pandas as pd
from lib.path import data_file_path


pyocr.tesseract.TESSERACT_CMD = r"C:/Program Files/Tesseract-OCR/tesseract.exe"


dir_path = os.chdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))
for file in os.listdir(dir_path):
    if file.endswith(".pdf"):
        tools = pyocr.get_available_tools()
        if len(tools) == 0:
            print("No OCR tool found!")
            sys.exit(1)
        tool = tools[0]
        print("Will use tool '%s'" % (tool.get_name()))

        langs = tool.get_available_languages()
        print("Available languages: %s" % ", ".join(langs))
        lang = langs[0]  # For English
        print("Will use language '%s'" % (lang))

        req_image = []
        final_text = {}

        image_pdf = Image(filename=file, resolution=200)
        image_jpeg = image_pdf.convert("pdf")

        for img in image_jpeg.sequence:
            img_page = Image(image=img)
            req_image.append(img_page.make_blob("jpeg"))

        i = 1
        for img in req_image:
            txt = tool.image_to_string(
                PI.open(io.BytesIO(img)),
                lang=lang,
                builder=pyocr.builders.TextBuilder(),
            )
            final_text[str(i)] = txt
            i += 1

        text = final_text.items()
        df = pd.DataFrame(text)
        file_name = file + ".txt"
        df.to_csv(
            data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs/" + file_name)
        )

        # supervised spacy model is produced via manual labeling using Doccano

        #  output should be written to a new directory, such as "ocr_output"
