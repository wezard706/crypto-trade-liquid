class TimeManager:
    def __init__(self, timestamp):
        self.now = timestamp

    def forward_timestamp(self, seconds):
        self.now += seconds
