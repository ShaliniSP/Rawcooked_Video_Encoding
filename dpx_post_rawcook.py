# TODO: Add docstrings
# TODO: Add logging
# TODO: Add error handling

import mmap
import sys
import os
from datetime import datetime
from pathlib import Path

from utils.util_functions import check_mediaconch_policy, log, move_file

from scripts.config import SCRIPT_LOGS_DIR, MKV_POLICY_CHECK_FAILS, MKV_COOKED_PATH, MKV_POLICY_PATH, POST_RAWCOOK_FAILS, \
    MKV_COMPLETED_PATH, DPX_TO_COOK_PATH, DPX_TO_COOK_V2_PATH


# ====================================================================
# === Clean up and inspect logs for problem DPX sequence encodings ===
# ====================================================================


class DpxPostRawcook:
    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOGS_DIR, "dpx_post_rawcook.log")
        self.date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.mkv_path_set = set()
        self.txt_path_set = set()
        self.missing_txt_files = set()
        self.missing_mkv_files = set()

    def check_missing(self):
        """Checks whether both mkv and txt file is present in the rawcooked folder"""
        try:
            for mkv_path in self.mkv_path_set:
                txt_file_path = str(Path(mkv_path).with_suffix(".mkv.txt"))
                if txt_file_path not in self.txt_path_set:
                    self.missing_txt_files.add(txt_file_path)

            for txt_path in self.txt_path_set:
                mkv_file_path = str(Path(txt_path).with_suffix(""))
                if mkv_file_path not in self.mkv_path_set:
                    self.missing_mkv_files.add(mkv_file_path)

            for file in self.missing_mkv_files:
                print(f"MKV file not found: {file}")

            for file in self.missing_txt_files:
                print(f"TXT file not found:  {file}")
        except Exception as e:
            print(f"Error: {e}")

    def process(self):
        """Initiates the Post Rawcooked Workflow
        Creates the temporary and log files (if not present)
        Also checks if the mkv_cooked folder has any files to process
        """
        try:
            if not os.path.exists(self.logfile):
                with open(self.logfile, 'w+'):
                    pass

            # Check mkv_cooked/ folder populated before starting log writes
            if any(os.scandir(MKV_COOKED_PATH)):
                log(self.logfile, "===================== Post-RAWcook workflows STARTED =====================")
                log(self.logfile, "Files present in mkv_cooked folder, checking if ready for processing...")

                self.mkv_path_set = set([f.path for f in os.scandir(MKV_COOKED_PATH) if f.name.endswith(".mkv")])
                self.txt_path_set = set([f.path for f in os.scandir(MKV_COOKED_PATH) if f.name.endswith(".mkv.txt")])

            else:
                print("MKV folder empty, script exiting")
                raise FileNotFoundError("No MKV files to be checked")
        except FileNotFoundError as e:
            print(f"Error: {e}")
        except OSError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error occurred: {e}")

    def check_mkv_policies(self):
        """For every .mkv file it checks against the mkv policy using mediaconch
        Scans through the mkv_cooked folder for .mkv files and runs mediaconch.
        If a file fails the check, the path of that file and the respective .mkv.txt file get appended into a
        temporary .txt file named temp_mediaconch_policy_fails.txt
        """
        for mkv_path in self.mkv_path_set.copy():
            try:
                mkv_file_name = Path(mkv_path).name
                if not check_mediaconch_policy(MKV_POLICY_PATH, mkv_path):
                    log(self.logfile, f"FAIL: RAWcooked MKV {mkv_file_name} has failed the mediaconch policy")
                    txt_file_path = Path(mkv_path).with_suffix(".mkv.txt")
                    txt_file_name = txt_file_path.name
                    folder_name = f"{mkv_file_name.split('.')[0]}/"
                    move_path = os.path.join(MKV_POLICY_CHECK_FAILS, folder_name)
                    if not os.path.exists(move_path):
                        os.mkdir(move_path)

                    move_file(mkv_path, os.path.join(move_path, mkv_file_name))
                    self.mkv_path_set.remove(mkv_path)

                    if str(txt_file_path) in self.txt_path_set:
                        move_file(txt_file_path, os.path.join(move_path, txt_file_name))
                        self.txt_path_set.remove(str(txt_file_path))
                    else:
                        raise FileNotFoundError(f"Missing txt file: {Path(txt_file_path).name}")
            except (FileNotFoundError, OSError) as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"Error occurred: {e}")

    def check_general_errors(self):
        """Checks the mediaconch passed .mkv files for any other RAWCooked errors
        Reads each of the .mkv.txt files and checks for messages stored in error_messages
        If an error is found, the respective .mkv file is moved to dpx_for_review/post_rawcook_fails/mkv_files/
        and the .mkv.txt files are moved to dpx_for_review/post_rawcook_fails/rawcook_output_logs/
        """
        error_messages = [
            b"Reversibility was checked, issues detected, see below.",
            b"Error:", b"Conversion failed!",
            b"Please contact info@mediaarea.net if you want support of such content."
        ]

        error_file_path_list = []

        for txt_file_path in self.txt_path_set:
            try:
                with open(txt_file_path, 'rb', 0) as file:
                    s = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                    for msg in error_messages:
                        if s.find(msg) != -1:
                            error_file_path_list.append(txt_file_path)
                            break
            except (FileNotFoundError, OSError) as e:
                print(f"Error reading file: {e}")

        for txt_file_path in error_file_path_list:
            try:
                mkv_file_path = Path(txt_file_path).with_suffix('')
                txt_file_name = Path(txt_file_path).name
                mkv_file_name = mkv_file_path.name

                folder_name = f"{txt_file_name.split('.')[0]}/"
                move_path = os.path.join(POST_RAWCOOK_FAILS, folder_name)
                if not os.path.exists(move_path):
                    os.mkdir(move_path)

                log(self.logfile, f"UNKNOWN ENCODING ERROR: {mkv_file_name} encountered error")
                log(self.logfile, f"Moving {mkv_file_name} and {txt_file_name} to post_rawcook_fails for manual review")

                move_file(txt_file_path, os.path.join(move_path, txt_file_name))
                self.txt_path_set.remove(txt_file_path)

                if str(mkv_file_path) in self.mkv_path_set:
                    move_file(mkv_file_path, os.path.join(move_path, mkv_file_name))
                    self.mkv_path_set.remove(mkv_file_path)
                else:
                    raise FileNotFoundError(f"Missing mkv file:{mkv_file_name}")
            except (FileNotFoundError, OSError) as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"Error occurred: {e}")

    def move_mkv_completed(self):
        """Function to move the completed dpx_sequences to completed folder"""
        for mkv_path in self.mkv_path_set:
            try:
                mkv_file_name = Path(mkv_path).name
                txt_file_path = Path(mkv_path).with_suffix(".mkv.txt")
                txt_file_name = txt_file_path.name
                log(self.logfile,
                    f"Checks passed. Moving {mkv_file_name} and {txt_file_name} to completed folder")
                folder_name = f"{mkv_file_name.split('.')[0]}/"
                move_path = os.path.join(MKV_COMPLETED_PATH, folder_name)
                if not os.path.exists(move_path):
                    os.mkdir(move_path)
                move_file(mkv_path, os.path.join(move_path, mkv_file_name))
                if str(txt_file_path) in self.txt_path_set:
                    move_file(txt_file_path, os.path.join(move_path, txt_file_name))
                else:
                    raise FileNotFoundError(f"Missing txt file: {txt_file_name}")
            except FileNotFoundError as e:
                print(f"Error: {e}")
            except Exception as e:
                print(f"Error occurred: {e}")

    def move_dpx_completed(self, dpx_to_cook_folder):
        """ Moves dpx sequences that are completed into the completed folder along with the framemd5 sequences """

        try:
            with os.scandir(dpx_to_cook_folder) as entries:
                log(self.logfile, f"Moving all processed dpx_folders to review/completed")
                for entry in entries:
                    try:
                        if entry.is_dir():
                            dpx_folder_name = str(entry.name)
                            move_path = os.path.join(MKV_COMPLETED_PATH, dpx_folder_name)
                            if not os.path.exists(move_path):
                                try:
                                    os.mkdir(move_path)
                                except OSError as e:
                                    print(f"Error creating directory: {e}")
                                    continue
                            move_dpx_path = os.path.join(move_path, dpx_folder_name + "_processed_dpx")

                            move_file(entry, move_dpx_path)
                            md5_path = Path(entry.path).with_suffix(".framemd5")
                            move_md5_path = os.path.join(move_path, md5_path.name)
                            if os.path.exists(md5_path):
                                move_file(md5_path, move_md5_path)
                            else:
                                raise FileNotFoundError(f"MD5 file does not exist for {dpx_folder_name}")
                    except FileNotFoundError as e:
                        print(f"Error: {e}")
        except OSError as e:
            print(f"Error scanning directory: {e}")

    def execute(self):
        try:
            self.process()
            self.check_missing()
            self.check_mkv_policies()
            self.check_general_errors()
            self.move_mkv_completed()
            self.move_dpx_completed(DPX_TO_COOK_PATH)
            self.move_dpx_completed(DPX_TO_COOK_V2_PATH)
        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("Workflow execution failed for post_rawcook")


if __name__ == '__main__':
    post_rawcook = DpxPostRawcook()
    post_rawcook.execute()
