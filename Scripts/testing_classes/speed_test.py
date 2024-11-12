import cProfile
import pstats
from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout
from recording_main import RecordingMain


def analyze_profile_data():
    # Load the profile data from the file
    stats = pstats.Stats('profile_data.prof')

    # Sort the statistics by cumulative time (total time spent in each function, including sub-functions)
    stats.sort_stats(pstats.SortKey.CUMULATIVE)

    # Print the top 10 functions with the highest cumulative time
    stats.print_stats(10)

    # Optionally, print more detailed information
    # Print the callers of the top 10 functions
    stats.print_callers(10)

    # Print the functions called by the top 10 functions
    stats.print_callees(10)

def main():
    # Create the Qt Application
    app = QApplication([])
    # Create and show the main application window
    main_window = RecordingMain()
    main_window.show()
    # Run the event loop
    app.exec()


if __name__ == "__main__":

    #cProfile.run('main()', 'profile_data.prof')
    analyze_profile_data()
