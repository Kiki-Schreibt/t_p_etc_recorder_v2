from datetime import datetime

from hot_disk_handler import HotDiskScheduleGrabber


def tmp_schedule_dir(tmp_path):
    # create a fake schedule directory with some .hseq files
    fn_good = tmp_path / "100_C_3_s_100_mW_Mica_ins_5465_type.hseq"
    fn_good.write_text("dummy")
    fn_other = tmp_path / "200_C_5_s_50_mW_Mica_ins_5465_type.hseq"
    fn_other.write_text("dummy")
    # a decoy file that should not match
    (tmp_path / "NOT_A_MATCH.hseq").write_text("dummy")
    return str(tmp_path)

def test_search_file_finds_exact_match(tmp_schedule_dir=None):
    grabber = HotDiskScheduleGrabber(
                                     sensor_insulation="Mica",
                                     sensor_type="5465-F1")

    # temperature=100, duration=3, power=100 should match fn_good
    path = grabber.search_file(temperature="100", heat_pulse_duration="3-5", heating_power="100-200")
    print(path)



def test_add_file_names_to_dict(tmp_schedule_dir):
    grabber = HotDiskScheduleGrabber(template_folder_path=tmp_schedule_dir,
                                     sensor_insulation="Mica",
                                     sensor_type="5465")

    # supply two entries out of order, and with string timestamp
    schedules = [
        {
            "measurement_time": "2025-07-22 15:00:00",
            "temperature": 100,
            "heat_pulse_duration": 3,
            "heating_power": 100
        },
        {
            "measurement_time": "2025-07-22 14:00:00",
            "temperature": 200,
            "heat_pulse_duration": 5,
            "heating_power": 50
        }
    ]

    result = grabber.add_file_names_to_dict(schedules)

    # times should have been parsed to datetime and sorted ascending
    times = [m["measurement_time"] for m in result]
    assert all(isinstance(t, datetime) for t in times)
    assert times[0] < times[1]

    # the first entry is the 14:00 one, which matches the 200_C... file
    assert result[0]["file_path"].endswith("200_C_5_s_50_mW_Mica_ins_5465_type.hseq")
    # the second entry is the 15:00 one, matches the 100_C... file
    assert result[1]["file_path"].endswith("100_C_3_s_100_mW_Mica_ins_5465_type.hseq")


test_search_file_finds_exact_match()
