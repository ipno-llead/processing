import os
import deba


def remove_new_lines():
    directory = os.listdir(deba.data("raw/post/ocr"))
    os.chdir(deba.data("raw/post/ocr"))
    for file in directory:
        if file.endswith(".txt"):

            with open(file, "r", encoding="utf-8") as f:
                text = "\n".join(
                    [line.strip() for line in f.read().split("\n") if line.strip()]
                )

            with open(file, "w", encoding="utf-8") as o:
                o.write(text)
    

if __name__ == "__main__":
    df = remove_new_lines()
