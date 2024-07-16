# TODO: Add logging

import os
import subprocess

from scripts.config import SCRIPT_LOGS_DIR, DPX_GAP_CHECK_PATH, DPX_GAP_CHECK_FAILS, \
    DPX_POLICY_CHECK_PATH, DPX_POLICY_PATH, DPX_TO_COOK_PATH, DPX_TO_COOK_V2_PATH, DPX_POLICY_CHECK_FAILS, RAWCOOK_LICENSE
from utils.util_functions import create_file, log, find_dpx_folder_from_sequence, find_missing, \
    check_mediaconch_policy, move_file, find_folder_name_from_sequence


class DpxAssessment:
    def __init__(self, check_gaps=True, check_policy=True):
        # Log files
        self.logfile = os.path.join(SCRIPT_LOGS_DIR, 'dpx_assessment.log')

        # Sets the assessment folder based on whether user has requested gap checking
        self.assessment_folder = DPX_GAP_CHECK_PATH
        self.check_gaps = check_gaps
        self.check_policy = check_policy
        if not check_gaps:
            if not check_policy:
                print("No preprocessing")
                self.assessment_folder = ''
            else:
                self.assessment_folder = DPX_POLICY_CHECK_PATH

        # Set of folders containing dpx files
        self.dpx_to_assess = set()

    def process(self) -> None:
        """Initiates the workflow

        Checks if DPX sequences are present in the DPX_PATH and creates the temporary files needed for the workflow.
        Exits the script if there are no DPX files in the path specified in DPX_PATH
        """
        try:
            # Check whether gap checking or policy is enabled
            if not self.check_gaps and not self.check_policy:
                print("No preprocessing required, script exiting")
                raise RuntimeError("No preprocessing required, script exiting")

            # Check for DPX sequences in path before script launch
            if not os.listdir(self.assessment_folder):
                raise FileNotFoundError("No files available for encoding")
        except Exception as e:
            print(f"Error: {e}")

        try:
            create_file(self.logfile)
        except Exception as e:
            print(f"Error: {e}")

        log(self.logfile, "\n============= DPX Assessment workflow START =============\n")

    def find_dpx_to_assess(self):
        """Finds the main folders containing dpx files in the assessment folder"""

        try:
            for seq in os.listdir(self.assessment_folder):
                seq_path = os.path.join(self.assessment_folder, seq)
                if not os.path.isdir(seq_path):
                    continue
                dpx_folder = find_dpx_folder_from_sequence(seq_path)
                if dpx_folder:
                    self.dpx_to_assess.add(dpx_folder)
            for s in self.dpx_to_assess:  # TODO: Change this for loop to logging
                print(s)

        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("Failed to find DPX folders") from e

    def gap_check(self):
        """Function to check for gaps in the dpx sequences"""

        try:
            # TODO: Add logging
            for seq in self.dpx_to_assess.copy():
                has_gaps = find_missing(seq)
                folder_name = find_folder_name_from_sequence(seq, self.assessment_folder)
                if has_gaps:
                    print("GAPS PRESENT IN FOLDERS:")
                    # Renaming the folder before moving to failed folder
                    source_path = os.path.join(self.assessment_folder, folder_name)
                    dest_path = os.path.join(DPX_GAP_CHECK_FAILS, folder_name)
                    move_file(source_path, dest_path)

                else:
                    # Moving to policy check
                    source_path = os.path.join(self.assessment_folder, folder_name)
                    dest_path = os.path.join(DPX_POLICY_CHECK_PATH, folder_name)
                    move_file(source_path, dest_path)
                    self.dpx_to_assess.add(seq.replace(DPX_GAP_CHECK_PATH, DPX_POLICY_CHECK_PATH))

                self.dpx_to_assess.remove(seq)

            # Setting the new assessment folder
            self.assessment_folder = DPX_POLICY_CHECK_PATH

        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("Gap checking failed") from e

    def check_v2(self):
        """Executes Rawcooked to check if the sequence generates a large reversibility file and moves it out of the
        policy folder """

        try:
            if not self.dpx_to_assess:
                print("Nothing to assess. Script Exiting")
                raise FileNotFoundError("No sequences to assess")

            for seq in self.dpx_to_assess.copy():
                folder_name = find_folder_name_from_sequence(seq, self.assessment_folder)
                check_v2_folder = os.path.join(self.assessment_folder, folder_name)
                command = ['rawcooked', '--license', RAWCOOK_LICENSE, '--check', '--no-encode', check_v2_folder]
                log(self.logfile,
                    f"Checking for large reversibility file issue in {seq}")

                subprocess_logs = []
                with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as p:
                    for line in p.stderr:  # TODO: Fix print statements and change to logging
                        subprocess_logs.append(line)
                        print(line)
                    for line in p.stdout:
                        subprocess_logs.append(line)
                        print(line)

                std_logs = ''.join(subprocess_logs)

                # Checks for sequences with large reversibility file
                if 'Error: the reversibility file is becoming big' in std_logs:
                    log(self.logfile,
                        f"FAIL: {seq} REVERSIBILITY FILE IS TOO BIG. Moving to v2 processing folder")
                    folder_name = find_folder_name_from_sequence(seq, self.assessment_folder)
                    source_path = os.path.join(self.assessment_folder, folder_name)
                    dest_path = os.path.join(DPX_TO_COOK_V2_PATH, folder_name)
                    move_file(source_path, dest_path)
                    self.dpx_to_assess.remove(seq)
        except FileNotFoundError:
            print(f"No sequences to process in: {self.assessment_folder}")
        except subprocess.CalledProcessError as cpe:
            print(f"CalledProcessError: {cpe}")
            raise RuntimeError("Rawcooked execution failed") from cpe
        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("An unexpected error occurred during Rawcooked execution") from e

    def move_v1(self) -> None:
        """Moves remaining sequences in the assessment folder to dpx_to_cook"""

        try:
            if not self.dpx_to_assess:
                print("Nothing to move. Script Exiting")
                raise FileNotFoundError("No sequences left to move")

        except FileNotFoundError:
            print(f"No sequences to process in: {self.assessment_folder}")

        try:
            for seq in self.dpx_to_assess.copy():
                folder_name = find_folder_name_from_sequence(seq, self.assessment_folder)
                source_path = os.path.join(self.assessment_folder, folder_name)
                dest_path = os.path.join(DPX_TO_COOK_PATH, folder_name)
                move_file(source_path, dest_path)
        except Exception as e:
            print(f"Error: {e}")

    def check_dpx_policy(self) -> None:
        """Checks if the dpx files passed as parameters match mediaconch policies"""

        try:
            if not self.dpx_to_assess:
                print("Nothing to assess. Script Exiting")
                raise FileNotFoundError("No sequences to assess")

            for seq in self.dpx_to_assess.copy():
                # Get one dpx file from each sequence to check against mediaconch policy
                dpx_file = None
                with os.scandir(seq) as entries:
                    for entry in entries:
                        if entry.is_file() and entry.name.endswith('.dpx'):
                            dpx_file = entry.name
                            break

                folder_name = find_folder_name_from_sequence(seq, self.assessment_folder)
                log(self.logfile, f"Metadata file creation has started for: {dpx_file}")
                if not check_mediaconch_policy(DPX_POLICY_PATH, os.path.join(seq, dpx_file)):
                    log(self.logfile,
                        f"FAIL: {dpx_file} DOES NOT CONFORM TO MEDIACONCH POLICY. Moving to dpx policy "
                        f"failed folder")
                    source_path = os.path.join(self.assessment_folder, folder_name)
                    dest_path = os.path.join(DPX_POLICY_CHECK_FAILS, folder_name)
                    move_file(source_path, dest_path)
                    self.dpx_to_assess.remove(seq)

        except FileNotFoundError:
            print(f"No sequences to assess in {self.assessment_folder}")
        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("An unexpected error occurred during policy checking") from e

    def execute(self) -> None:
        """Executes the workflow step by step as:

        1. process(): Checks if .dpx files are present in the input folder and creates temporary files
        2. find_dpx_to_assess(): Finds the lowest folder containing dpx sequences at any depth and updates the class variable dpx_to_assess
        3. gap_check(): Checks if dpx sequences have incoherent gaps and moves them to dpx_to_review/gap_check_fails
        4. check_v2(): It takes the dict and runs rawcooked to check if there is are large reversibility files. Removes
        the sequence from the dict and moves it to the v2 processing folder
        5. check_policy(): Check a randomly chosen .dpx file from each sequence
        against mediaconch policies. If it fails moves it to review/dpx_policy_check_fails
        6. Moves the remaining sequences to dpx_to_cook folder else moves to
        """

        try:
            self.process()
            if self.check_gaps:
                self.find_dpx_to_assess()
                self.gap_check()
            if self.check_policy:
                self.find_dpx_to_assess()
                self.check_v2()
                self.check_dpx_policy()
            self.move_v1()
        except Exception as e:
            print(f"Error: {e}")
            raise RuntimeError("Workflow execution failed for assessment")


if __name__ == '__main__':
    dpx_assessment = DpxAssessment()
    dpx_assessment.execute()
