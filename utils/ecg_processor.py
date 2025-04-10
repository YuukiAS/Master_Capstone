import os
import numpy as np
import logging
import sys
sys.path.append('.')

import xmltodict
from xml.parsers.expat import ExpatError
import datetime

import neurokit2 as nk

logger = logging.getLogger(__name__)

class ECG_Reader:
    """
    Extract lead signals from a CardioSoftECG XML file
    """
    def __init__(self, path, encoding="ISO8859-1"):
        self.path = path

        with open(path, "rb") as xml:
            try:
                self.data = xmltodict.parse(xml.read().decode(encoding))["CardiologyXML"]
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
            self.maxWorkload = self.data["ExerciseMeasurements"]["MaxWorkload"]  # unit: Watts

    def get_lead_signals(self):
        """
        Get the lead signals from the ECG data. 
        """
        # define
        # ArrhythmiaData field only records arrhythmic events
        # FullDisclosure field records complete ECG data for the entire test
        if "FullDisclosure" not in self.data:
            raise ValueError("FullDisclosure field should exist in the ECG XML file")

        full_disclosure = self.data["FullDisclosure"]

        try:
            startTime = full_disclosure["StartTime"]
            startTime = float(startTime["Minute"]) * 60 + float(startTime["Second"])  # unit: seconds
            leadOrder = full_disclosure["LeadOrder"].split(",")
            # print(f"Lead order: {leadOrder}")
        except TypeError:
            raise ValueError("FullDisclosure field is not in correct format")

        # define 
        # All full-disclosure samples separated by comma in the following order:
        # Sample-1 Lead-1, Sample-1 Lead-2 … Sample-1 Lead NumberOfChannels,
        # Sample-2 Lead-1, Sample-2 Lead-2 … Sample-2 Lead NumberOfChannels,
        # Sample-3 Lead-1, Sample-3 Lead-2 … Sample-3 Lead NumberOfChannels

        raw_signals = full_disclosure["FullDisclosureData"].split(",")
        raw_signals = [signal for signal in raw_signals if signal != '']  # remove ''

        signals = {}
        for i, lead in enumerate(leadOrder):
            signals[lead] = np.array(raw_signals[i::len(leadOrder)], dtype=int)
        
        return signals

    def get_max_heart_rate(self):
        return self.maxHeartRate

    def get_max_workload(self):
        return self.maxWorkload

class ECG_Processor:
    """
    Process the ECG data for a subject.
    """
    def __init__(self, data_dir, subject, sampling_rate=500):
        if not isinstance(subject, str):
            raise TypeError("Subject must be a string")
        self.subject = subject
        self.data_dir = data_dir
        self.sampling_rate = sampling_rate

        self.signals = None
        if self.check_data():
            self._load_data()

    def _load_data(self):
        """
        Load the ECG data for the subject.
        """
        if not self.check_data():
            raise FileNotFoundError("ECG data does not exist for the subject")

        xml_file = os.path.join(self.data_dir, f"{self.subject}_6025_0_0.xml")
        try:
            data_reader = ECG_Reader(xml_file)
            self.signals = data_reader.get_lead_signals()  # load all leads
            self.max_heart_rate = data_reader.get_max_heart_rate()
            self.max_workload = data_reader.get_max_workload()
        except ValueError as e:
            raise ValueError(f"Subject {self.subject}: {e}")

    def check_data(self):
        """
        Check if the ECG data exists for the subject.
        Return True if the data exists, False otherwise.
        """
        return os.path.exists(os.path.join(self.data_dir, f"{self.subject}_6025_0_0.xml"))

    def get_info(self):
        return {
            "max_heart_rate": self.max_heart_rate,
            "max_workload": self.max_workload,
            "sampling_rate": self.sampling_rate
        }

    def get_signals(self, lead=None):
        """
        Get the ECG data for the subject.
        If lead is not specified, signals from all leads will be returned in a dictionary.
        If a lead is specified, a numpy array will be returned.
        """        
        if not lead:
            return self.signals
        else:
            return self.signals[lead]