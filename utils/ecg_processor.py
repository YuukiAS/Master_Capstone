import os
import numpy as np
import matplotlib.pyplot as plt
import xmltodict
from xml.parsers.expat import ExpatError
import datetime
import neurokit2 as nk

from .constants import DatabaseConfig


class ECG_Reader:
    """
    Extract lead signals and metadata from a CardioSoftECG XML file.
    For detailed information, please refer to the manual:
    https://image.tigermedical.com/Manuals/GEH2062898-001-504227--20180315070115755.pdf

    This class parses XML files containing ECG data, extracting both the signal data
    and relevant metadata such as observation time, heart rate, and workload.

    Attributes:
        path (str): Path to the XML file
        data (dict): Parsed XML data
        observationDateTime (datetime): When the ECG was recorded
        maxHeartRate (float): Maximum heart rate during test
        maxPredictedHR (float): Predicted maximum heart rate
        maxWorkload (float): Maximum workload in Watts

    Raises:
        ValueError: If XML file is empty or malformed
        ExpatError: If XML parsing fails
    """

    def __init__(self, path, encoding="ISO8859-1"):
        """
        Initialize ECG reader with file path.

        Args:
            path (str): Path to the XML file
            encoding (str, optional): XML file encoding. Defaults to "ISO8859-1"
        """
        self.path = path

        with open(path, "rb") as xml:
            try:
                self.data = xmltodict.parse(xml.read().decode(encoding))[
                    "CardiologyXML"
                ]
            except ExpatError:  # In this case, the XML file is empty
                raise ValueError("XML file is empty")

            self.observationDateTime = datetime.datetime(
                year=int(self.data["ObservationDateTime"]["Year"]),
                month=int(self.data["ObservationDateTime"]["Month"]),
                day=int(self.data["ObservationDateTime"]["Day"]),
                hour=int(self.data["ObservationDateTime"]["Hour"]),
                minute=int(self.data["ObservationDateTime"]["Minute"]),
                second=int(self.data["ObservationDateTime"]["Second"]),
            )
            self.maxHeartRate = self.data["ExerciseMeasurements"]["MaxHeartRate"]
            self.maxPredictedHR = self.data["ExerciseMeasurements"]["MaxPredictedHR"]
            self.maxWorkload = self.data["ExerciseMeasurements"][
                "MaxWorkload"
            ]  # unit: Watts

            try:
                fullDisclosure = self.data["FullDisclosure"]
                self.startTime = float(
                    fullDisclosure["StartTime"]["Minute"]
                ) * 60 + float(fullDisclosure["StartTime"]["Second"])
            except KeyError:
                raise ValueError("FullDisclosure field is not in correct format")
            except TypeError:
                raise ValueError("StartTime field is missing or not in correct format")

    def get_start_time(self):
        """
        Get the start time of the ECG recording.

        Returns:
            float: Start time in seconds from the beginning of recording.
                  Calculated as (minutes * 60 + seconds) from the StartTime field.

        Note:
            This is different from observationDateTime, which gives the actual
            calendar time when the recording started. This method returns the
            offset in seconds from the start of the recording.
        """
        return self.startTime

    def get_lead_signals(self):
        """
        Extract ECG signals for all leads from the data.

        Returns:
            dict: Dictionary mapping lead names to numpy arrays of signal values

        Raises:
            ValueError: If FullDisclosure field is missing or malformed
            TypeError: If data format is incorrect
        """
        # define
        # Strip field store the 10-second ECG strips
        # ArrhythmiaData field only records arrhythmic events
        # FullDisclosure field records complete ECG data for the entire test
        if "FullDisclosure" not in self.data:
            raise ValueError("FullDisclosure field should exist in the ECG XML file")

        full_disclosure = self.data["FullDisclosure"]

        try:  # unit: seconds
            leadOrder = full_disclosure["LeadOrder"].split(",")
            # print(f"Lead order: {leadOrder}")
        except TypeError:
            raise ValueError("FullDisclosure field is not in correct format")

        # define All full-disclosure samples separated by comma in the following order:
        # Sample-1 Lead-1, Sample-1 Lead-2 … Sample-1 Lead NumberOfChannels,
        # Sample-2 Lead-1, Sample-2 Lead-2 … Sample-2 Lead NumberOfChannels,
        # Sample-3 Lead-1, Sample-3 Lead-2 … Sample-3 Lead NumberOfChannels

        raw_signals = full_disclosure["FullDisclosureData"].split(",")
        raw_signals = [signal for signal in raw_signals if signal != ""]  # remove ''

        signals = {}
        for i, lead in enumerate(leadOrder):
            signals[lead] = np.array(raw_signals[i :: len(leadOrder)], dtype=int)

        return signals

    def get_max_heart_rate(self):
        """
        Get maximum heart rate recorded during test.

        Returns:
            float: Maximum heart rate value
        """
        return self.maxHeartRate

    def get_max_workload(self):
        """
        Get maximum workload recorded during test.

        Returns:
            float: Maximum workload in Watts
        """
        return self.maxWorkload


class ECG_Processor:
    """
    Process and analyze ECG data for a subject.

    This class handles loading, validation, and processing of ECG data files.
    It provides access to both raw signals and derived measurements.

    All bicycle protocols consist, in order, of:
    - Initial 15 seconds rest (pretest resting ECG)
    - 2 minute phase at constant power
    - Linear increase over 4 minutes from Start to Peak power level
    - Concluded by a 1 minute recovery period.

    Attributes:
        subject (str): Subject identifier
        data_dir (str): Directory containing ECG files
        sampling_rate (int): Signal sampling rate in Hz
        signals (dict): Dictionary of lead signals
        max_heart_rate (float): Maximum heart rate during test
        max_workload (float): Maximum workload in Watts

    Raises:
        TypeError: If subject ID is not a string
        FileNotFoundError: If ECG file doesn't exist
        ValueError: If ECG data is invalid
    """

    def __init__(self, data_dir, subject, sampling_rate=DatabaseConfig.SAMPLING_RATE):
        """
        Initialize ECG processor for a subject.

        Args:
            data_dir (str): Directory containing ECG files
            subject (str): Subject identifier
            sampling_rate (int, optional): Sampling rate in Hz.
                Defaults to DatabaseConfig.SAMPLING_RATE
        """
        if not isinstance(subject, str):
            if not isinstance(subject, (int, float)):
                raise TypeError(
                    f"Subject ID must be a string, integer, or float, got {type(subject)}"
                )
            subject = str(subject)
        self.subject = subject
        self.data_dir = data_dir
        self.sampling_rate = sampling_rate

        self.signals = None
        if self.check_data():
            self._load_data()
            print(f"ECG Processor initialized for subject {self.subject}")

    def _load_data(self):
        """
        Load and validate ECG data from XML file.

        Raises:
            FileNotFoundError: If ECG file doesn't exist
            ValueError: If ECG data is invalid or malformed
        """
        if not self.check_data():
            raise FileNotFoundError("ECG data does not exist for the subject")

        xml_file = os.path.join(self.data_dir, f"{self.subject}_6025_0_0.xml")
        try:
            xml_reader = ECG_Reader(xml_file)
            self.signals = xml_reader.get_lead_signals()  # load all leads

            signal_length = len(self.signals["I"]) / self.sampling_rate

            self.max_heart_rate = xml_reader.get_max_heart_rate()
            self.max_workload = xml_reader.get_max_workload()

            start_time = xml_reader.get_start_time()

            # We follow the stage name in data field 5988. Unit: seconds
            self.stage_time = {}
            self.stage_time["steady"] = {"start": 0, "end": 15 - start_time}
            self.stage_time["constant"] = {
                "start": 15 - start_time,
                "end": 15 + 60 * 2 - start_time,
            }
            self.stage_time["ramp"] = {
                "start": 15 + 60 * 2 - start_time,
                "end": 15 + 60 * 2 + 60 * 4 - start_time,
            }
            if 15 + 60 * 2 + 60 * 4 - start_time > signal_length:
                raise ValueError(
                    "ECG signal length is not enough. The ECG may not contain all phases."
                )
            self.stage_time["noload"] = {
                "start": 15 + 60 * 2 + 60 * 4 - start_time,
                "end": signal_length,
            }

        except ValueError as e:
            raise ValueError(f"Subject {self.subject}: {e}")

    def check_data(self):
        """
        Check if ECG data file exists for the subject.

        Returns:
            bool: True if data file exists, False otherwise
        """
        return os.path.exists(
            os.path.join(self.data_dir, f"{self.subject}_6025_0_0.xml")
        )

    def get_info(self):
        """
        Get metadata about the ECG recording.

        Returns:
            dict: Dictionary containing:
                - max_heart_rate (float): Maximum heart rate
                - max_workload (float): Maximum workload in Watts
                - sampling_rate (int): Signal sampling rate in Hz
        """
        return {
            "max_heart_rate": self.max_heart_rate,
            "max_workload": self.max_workload,
            "sampling_rate": self.sampling_rate,
        }

    def get_raw_signals(self, lead=None):
        """
        Get ECG signals for a specified lead or all leads.

        Args:
            lead (str, optional): Specific lead to return. If None, returns all leads.

        Returns:
            Union[dict, np.ndarray]: If lead is None, returns dictionary mapping lead
                names to signal arrays. If lead is specified, returns signal array
                for that lead.
        """
        if lead not in ["I", "2", "3"]:
            raise ValueError(f"Invalid lead: {lead}")
        if not lead:
            return self.signals
        else:
            return self.signals[lead]

    def get_signal_stage(self, stage_name, lead="2"):
        """
        Get ECG signal for a specific stage of the test.

        Args:
            stage_name (str): Name of the stage to extract. Must be one of:
                - 'steady': Initial 15 seconds rest (pretest resting ECG)
                - 'constant': 2 minute phase at constant power
                - 'ramp': Linear increase over 4 minutes from Start to Peak power
                - 'noload': 1 minute recovery period
            lead (str, optional): Lead to extract signal from. Defaults to "2"

        Returns:
            np.ndarray: Signal values for the specified stage and lead

        Raises:
            ValueError: If stage_name is not valid

        Note:
            Duration of each stage is adjusted based on the start_time offset
        """
        if stage_name not in self.stage_time.keys():
            raise ValueError(f"Invalid stage: {stage_name}")

        start_time = self.stage_time[stage_name]["start"]
        end_time = self.stage_time[stage_name]["end"]

        lead_signal = self.get_raw_signals(lead)
        lead_signal_stage = lead_signal[
            int(start_time * self.sampling_rate) : int(end_time * self.sampling_rate)
        ]
        print(
            f"Lead {lead} {stage_name} signal duration: {len(lead_signal_stage) / self.sampling_rate} seconds"
        )

        return lead_signal_stage

    def process_signal(self, stage_name, lead="2"):
        """
        Process ECG signal for a specific stage and calculate HRV metrics.

        Args:
            stage_name (str): Name of the stage to process. Must be one of:
                - 'steady': Initial 15 seconds rest (pretest resting ECG)
                - 'constant': 2 minute phase at constant power
                - 'ramp': Linear increase over 4 minutes from Start to Peak power
                - 'noload': 1 minute recovery period
            lead (str, optional): Lead to process. Defaults to "2"

        Returns:
            tuple: Three DataFrames containing:
                - hrv_time: Time-domain HRV metrics
                - hrv_freq: Frequency-domain HRV metrics
                - hrv_nonlinear: Non-linear HRV metrics

        Raises:
            ValueError: If stage_name is not valid

        Note:
            Uses neurokit2 for signal processing and HRV calculation
            See: https://neuropsychology.github.io/NeuroKit/functions/hrv.html
        """
        if stage_name not in self.stage_time.keys():
            raise ValueError(f"Invalid stage: {stage_name}")

        lead_signal_stage = self.get_signal_stage(stage_name, lead)

        # Start processing the signal
        peaks, _ = nk.ecg_process(
            lead_signal_stage, sampling_rate=self.sampling_rate
        )  # clean + peak detection + HR calculation + Quality assessment + QRS Complex delineation

        # Calculate HRV metrics
        hrv_time = nk.hrv_time(peaks, sampling_rate=self.sampling_rate)
        hrv_freq = nk.hrv_frequency(peaks, sampling_rate=self.sampling_rate)
        hrv_nonlinear = nk.hrv_nonlinear(peaks, sampling_rate=self.sampling_rate)

        print(f"Subject {self.subject}: Successfully processed {stage_name} signal for lead {lead}")
        return hrv_time, hrv_freq, hrv_nonlinear

    @staticmethod
    def plot_ecg_signal(signal, time=None, sampling_rate=500):
        """
        Plot ECG signal with a standardized grid layout.

        This method creates a plot of ECG signal with standard ECG grid formatting:
        - Major grid lines in red at 0.2s intervals
        - Minor grid lines in black at 0.04s intervals
        - Time axis in milliseconds
        - Automatic figure sizing based on signal length

        Args:
            signal (np.ndarray): ECG signal values
            time (np.ndarray, optional): Time points corresponding to signal values.
                If None, time will be generated based on sampling rate.
            sampling_rate (int, optional): Sampling frequency in Hz.
                Used when time is not provided. Defaults to 500.

        Returns:
            tuple: (matplotlib.figure.Figure, matplotlib.axes.Axes)
                The created figure and axes objects

        Raises:
            ValueError: If time is provided and its length doesn't match signal length

        Note:
            Grid implementation based on standard ECG paper:
            - Major grid lines at 0.2s intervals
            - Minor grid lines at 0.04s intervals
            Reference: https://www.indigits.com/post/2022/10/ecg_python/
        """
        if time is not None:
            if len(time) != len(signal):
                raise ValueError("Time and signal should have the same length")
        else:
            time = np.arange(len(signal)) / sampling_rate * 1000  # unit: ms

        fig = plt.figure(figsize=(max(15, len(time) / sampling_rate * 10), 3))
        ax = plt.axes()
        ax.plot(time, signal)

        # Setup major and minor ticks
        min_t = int(np.min(time))
        max_t = int(np.max(time))
        major_ticks = np.arange(min_t, max_t + 1, sampling_rate / 10)
        ax.set_xticks(major_ticks)

        min_y = np.min(signal)
        max_y = np.max(signal)
        minor_ticks = np.arange(min_y, max_y + 1, 10)
        ax.set_yticks(minor_ticks)

        # Make the major grid
        ax.grid(which="major", linestyle="-", color="red", linewidth="1.0")
        # Make the minor grid
        ax.grid(which="minor", linestyle=":", color="black", linewidth="0.5")

        plt.xlabel("Time (ms)")
        plt.ylabel("Amplitude")
        return fig, ax
