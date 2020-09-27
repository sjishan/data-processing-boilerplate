from . import types


class Packet:
    def describe(self, flavor):
        if flavor == "athena":
            return ",\n".join(
                [
                    f"{attr} {getattr(value, flavor)}"
                    for attr, value in self.__dict__.items()
                ]
            )
        elif flavor == "druid":
            results = []

            for attr, value in self.__dict__.items():
                if attr == "timestamp":
                    continue

                dtype = getattr(value, flavor)

                if dtype == "string":
                    results.append(attr)
                else:
                    results.append({"name": attr, "type": dtype})

            return results
        elif flavor in ("elasticsearch", "pandas"):
            return [
                {"name": attr, "type": getattr(value, flavor)}
                for attr, value in self.__dict__.items()
            ]
        else:
            raise RuntimeError(f"Schema type {flavor} does not exist")


class BasePacket(Packet):
    def __init__(self):
        self.timestamp = types.Timestamp()

        self.attribute_1 = types.Boolean()
        self.attribute_2 = types.Long()


class data_stream_1(BasePacket):
    def __init__(self):
        super(data_stream_1, self).__init__()

        self.attribute_3 = types.String()