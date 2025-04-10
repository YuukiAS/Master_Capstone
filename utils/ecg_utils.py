from datetime import date
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import numpy as np

def parse_date(ecg_xml_path):
    tree = ET.parse(ecg_xml_path)
    root = tree.getroot()

    obs_datatime = root.find("ObservationDateTime")

    day = int(obs_datatime.find("Day").text)
    month = int(obs_datatime.find("Month").text)
    year = int(obs_datatime.find("Year").text)

    return date(year, month, day)

def plot_ecg_signal(signal, time=None, sampling_rate=500):
        """
        We directly use the code provided in https://www.indigits.com/post/2022/10/ecg_python/.
        Time and signal should be two numpy arrays of equal length. Time should be in unit seconds.
        """
        if time is not None:
            if len(time) != len(signal):
                raise ValueError("Time and signal should have the same length")
        else:
            time = np.arange(len(signal)) / sampling_rate * 1000  # unit: ms

        fig = plt.figure(figsize=(max(15, len(time) / sampling_rate * 10), 3))
        ax = plt.axes()
        ax.plot(time, signal)
        # setup major and minor ticks
        min_t = int(np.min(time))
        max_t = int(np.max(time))
        major_ticks = np.arange(min_t, max_t + 1, sampling_rate / 10)
        ax.set_xticks(major_ticks)
        # Turn on the minor ticks on
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