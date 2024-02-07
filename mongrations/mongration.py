from mongrations.phase import Phase


class Mongration:
    _phases = list[Phase]()

    def phase(self, name):
        ph = Phase(name)
        self._phases.append(ph)
        return ph

    def phases(self) -> list[Phase]:
        return self._phases
