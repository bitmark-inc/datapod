import time
import os
import array
import json
import zipfile
import argparse
import shutil

FAKED_DIRECTORY_PREFIX = "/tmp/faked_data_archive-%d"
ARCHIVE_FILE = "/tmp/data.zip"


def removeContent(data, key=""):
    data_type = type(data).__name__
    if data_type == "list":
        for index, sub_data in enumerate(data):
            data[index] = removeContent(sub_data)
    elif data_type == "dict":
        for key in data:
            data[key] = removeContent(data[key], key)
    elif data_type == "int":
        if key.find("timestamp_ms") >= 0:
            return int(time.time() * 1000)
        elif key.find("timestamp") >= 0:
            return int(time.time())
        return 0
    elif data_type == "str" or data_type == "unicode":
        if key.startswith("ip") or key.endswith("ip"):
            return "8.8.8.8"
        elif key == "uri":
            return "photos_and_videos/blank.jpg"
        elif key == "url":
            return "https://www.google.com"
        elif key == "reaction":
            return data
        elif len(data) > 0:
            return data[0]
        return ""
    elif data_type == "float":
        return 0.0
    elif data_type == "bool":
        return False
    else:
        raise TypeError("unhandled type: " + data_type)
    return data


def create_blank_image(filename):
    width, height = 800, 600
    PPMheader = 'P6\n' + str(width) + ' ' + str(height) + '\n255\n'
    image = array.array('B', [255, 255, 255] * width * height)

    try:
        os.makedirs(os.path.dirname(filename))
    except:
        pass

    with open(filename, 'wb') as f:
        f.write(bytearray(PPMheader, 'ascii'))
        image.tofile(f)


def gen_fake_data(source_directory, archive_file):
    fake_directory = FAKED_DIRECTORY_PREFIX % int(time.time())
    create_blank_image(
        os.path.join(fake_directory, "photos_and_videos", "blank.jpg"))

    total_file_count = 1
    archived_file_count = 0

    for root, dir, files in os.walk(source_directory):
        if ".git" in root:
            continue
        try:
            os.makedirs(root.replace(source_directory, fake_directory))
        except:
            pass

        for filename in files:
            filepath = os.path.join(root, filename)
            fake_filepath = filepath.replace(source_directory, fake_directory)
            if filename.endswith(".json"):
                total_file_count += 1
                with open(filepath) as r:
                    rootData = json.loads(r.read())
                    with open(fake_filepath, 'w') as w:
                        json.dump(removeContent(rootData), w, indent=2)

    zipf = zipfile.ZipFile(archive_file, 'w', zipfile.ZIP_DEFLATED)
    for root, dirs, files in os.walk(fake_directory):
        for filename in files:
            filename = os.path.join(root, filename)
            fake_filepath = filename.replace(fake_directory, "")
            zipf.write(filename, fake_filepath)
            archived_file_count += 1
            print("Archive %s -> %s (%d / %d)" %
                  (filename, fake_filepath, archived_file_count,
                   total_file_count))
    zipf.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fake Facebook data generator')
    parser.add_argument("-s",
                        "--source-directory",
                        help="facebook data source",
                        dest="source",
                        default="")
    parser.add_argument("-o",
                        "--archive-file",
                        help="facebook fake data archive file path",
                        dest="archive_file",
                        default=ARCHIVE_FILE)
    args = parser.parse_args()

    source_dir = args.source
    archive_file = args.archive_file
    if not source_dir:
        raise ValueError("invalid data source")

    if not archive_file:
        raise ValueError("invalid archive file path")

    os.stat(source_dir)
    gen_fake_data(source_dir, archive_file)

    print("Archive file is generated at: %s" % archive_file)
