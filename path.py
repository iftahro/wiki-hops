class Path:
    """
    This class represents a wikipedia path between pages links.
    """

    def __init__(self, src, dst):
        self.history = [src]
        self.dst = dst
        self.last_title = self.history[-1]

    @classmethod
    def create_from_father(cls, father, title):
        c = cls(title, father.dst)
        c.history = [*father.history, title]
        return c

    def __eq__(self, other):
        return isinstance(other, Path) and self.last_title == other.last_title

    def __hash__(self):
        return hash(self.last_title)

    def __repr__(self):
        return self.last_title

    def __str__(self):
        return " ======> ".join([*self.history, self.dst])
