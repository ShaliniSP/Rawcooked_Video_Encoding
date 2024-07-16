# TODO: Store license key in a separate file and read from there

import concurrent.futures
import os
import subprocess

from utils.util_functions import log, create_file

from scripts.config import (SCRIPT_LOGS_DIR, RAWCOOKED_DIR, MKV_COOKED_PATH, DPX_TO_COOK_PATH, DPX_TO_COOK_V2_PATH, RAWCOOK_LICENSE)


class DpxRawcook:

    def __init__(self):
        self.logfile = os.path.join(SCRIPT_LOGS_DIR, "dpx_rawcook.log")
        self.mkv_cooked_folder = os.path.join(RAWCOOKED_DIR, "mkv_cooked/")
        # TODO: Take input from GUI later
        self.md5_checksum = True

    def process(self) -> None:
        """Initiates the workflow

        Creates log files if not present
        """
        try:
            create_file(self.logfile)

            # Write a START note to the logfile if files for encoding, else exit
            log(self.logfile, "============= DPX RAWcook script START =============")

        except Exception as e:
            print("Error creating logfile:", e)
            return

    def rawcooked_command_executor(self, start_folder_path: str, mkv_file_name: str, v2: bool) -> None:
        """The method passed to each process that executes rawcooked command

        Runs rawcooked command with respective parameters
        Stores the rawcooked console output to a .txt  file named as <mkv_file_name>.mkv.txt
        Checks if there are gaps in output v2 sequence, then that sequence is moved to rawcook fails folder
        """
        # string_command = f"rawcooked --license 004B159A2BDB07331B8F2FDF4B2F -y --all --no-accept-gaps {'--output-version 2' if v2 else ''} -s 5281680 {'--framemd5' if self.md5_checksum else ''} {start_folder_path} -o {MKV_COOKED_PATH}/{mkv_file_name}.mkv"
        string_command = (
            f"rawcooked --license {RAWCOOK_LICENSE} "
            f"-y --all --no-accept-gaps {'--output-version 2' if v2 else ''} "
            f"-s 5281680 {'--framemd5' if self.md5_checksum else ''} "
            f"{start_folder_path} -o {MKV_COOKED_PATH}/{mkv_file_name}.mkv"
        )
        output_txt_file = os.path.join(RAWCOOKED_DIR, "mkv_cooked", f"{mkv_file_name}.mkv.txt")
        command = string_command.split(" ")
        command = [c for c in command if len(c) > 0]
        command = list(command)
        print(command)
        subprocess_logs = []
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
            for line in p.stderr:
                subprocess_logs.append(line)
                print(f"{mkv_file_name} : {line}")
            for line in p.stdout:
                subprocess_logs.append(line)
                print(f"{mkv_file_name} : {line}")

        if p.returncode != 0:
            print("Rawcooked Command failed with error code:", p.returncode)
            raise RuntimeError("Failed to call rawcooked command")

        std_logs = ''.join(subprocess_logs)

        with open(output_txt_file, 'a+') as file:
            file.write(std_logs)

    def run_rawcooked(self, dpx_to_cook_folder_path, v2_flag) -> None:
        """Executes Rawcooked over the sequences present in the given dpx_folders

        First pass sequences have large reversibility file and thus needs to be cooked with --output-version 2 flag.
        Then runs Rawcooked over the sequences present in dpx_to_cook
        Runs Rawcooked with --framemd5 flag by default (might need to take user input later)
        """

        log(self.logfile, f"Checking for files in {dpx_to_cook_folder_path}")
        sequence_path_list = [os.path.join(dpx_to_cook_folder_path, folder) for folder in
                              os.listdir(dpx_to_cook_folder_path)]

        # Filter out only the folders as there can be .framemd5 files
        dpx_to_cook_folder_list = [sequence for sequence in sequence_path_list if os.path.isdir(sequence)]

        if len(dpx_to_cook_folder_list) == 0:
            log(self.logfile, f"No sequence found in {dpx_to_cook_folder_path}")
            return

        with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
            # Taking only 20 entries from the dictionary
            for seq_path in dpx_to_cook_folder_list[:20]:
                print(f"Cooking {seq_path}")
                if v2_flag:
                    log(self.logfile, f"{seq_path} will be cooked using RAWCooked V2")
                else:
                    log(self.logfile, f"{seq_path} will be cooked using RAWCooked")

                mkv_file_name = os.path.basename(seq_path)
                # Cooking with --framemd5 flag by default
                try:
                    executor.submit(self.rawcooked_command_executor, seq_path, mkv_file_name, v2_flag)
                except Exception as e:
                    print("Task Failed:", e)
                    raise RuntimeError(f"Failed to run rawcooked for {seq_path}") from e

    def execute(self):
        try:
            self.process()
            self.run_rawcooked(DPX_TO_COOK_V2_PATH, True)
            self.run_rawcooked(DPX_TO_COOK_PATH, False)
            log(self.logfile, "============= DPX RAWcook script END =============")
        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("Workflow execution failed for rawcooked")


if __name__ == '__main__':
    dpx_rawcook = DpxRawcook()
    dpx_rawcook.execute()
