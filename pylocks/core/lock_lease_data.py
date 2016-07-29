from pylocks import serialization

class LockLeaseData(serialization.Serializable):
    def __init__(self, request, id, acquired_at):
        self.request = request
        self.id = id
        self.acquired_at = acquired_at

    @property
    def key(self):
        return self.request.key

    def __eq__(self, other):
        return self.key == other.key and self.id == other.id

    def __hash__(self):
        return hash((self.key, self.id))

    def __repr__(self):
        return '<LockLeaseData key=%r id=%r />' % (self.key, self.id)

