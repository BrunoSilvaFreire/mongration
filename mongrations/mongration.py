from mongrations.phase import Phase


class Mongration:
    def __init__(self, name: str):
        self.name = name
        self._phases = list[Phase]()
        self._stateless = False

    def phase(self, name):
        ph = Phase(name)
        self._phases.append(ph)
        return ph

    def phases(self) -> list[Phase]:
        return self._phases

    def is_stateless(self):
        return self._stateless

    def is_stateful(self):
        return not self._stateless

    def mark_stateless(self):
        self._stateless = True
