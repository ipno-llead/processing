from wand.image import Image as WImage
from PIL import Image as PI
import io
import os
import pyocr
import pyocr.builders
import pandas as pd
import deba

def ocr_pdfs():
    pyocr.tesseract.TESSERACT_CMD = r"C:/Program Files/Tesseract-OCR/tesseract.exe"
    dir_path = os.chdir(deba.data("raw/post/post_officer_history/input"))
    for file in os.listdir(dir_path):
        if file.endswith(".pdf"):
            tools = pyocr.get_available_tools()
            if len(tools) == 0:
                raise Exception("No tool available")

            tool = tools[0]
            print("Will use tool '%s'" % (tool.get_name()))
            langs = tool.get_available_languages()
            print("Available languages: %s" % ", ".join(langs))
            lang = langs[0]  # For English
            print("Will use language '%s'" % (lang))

            req_image = []
            final_text = {}

            with WImage(filename=file, resolution=600) as image_pdf:
                image_jpeg = image_pdf.convert("pdf")

            try:
                for img in image_jpeg.sequence:
                    img_page = WImage(image=img)
                    req_image.append(img_page.make_blob("jpeg"))
            finally:
                image_jpeg.destroy()

            i = 0
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
            return df, file_name


if __name__ == "__main__":
    df, file_name = ocr_pdfs()
    df.to_csv(deba.data("ocr/post_officer_history/") + file_name)
