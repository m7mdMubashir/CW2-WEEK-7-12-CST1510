class SecurityIncident:
    def __init__(self, id, title, severity, status, date, resolved_date=None):
        self.id = id
        self.title = title
        self.severity = severity
        self.status = status
        self.date = date
        self.resolved_date = resolved_date

    def is_critical(self):
        return self.severity == "High" and self.status == "Open"