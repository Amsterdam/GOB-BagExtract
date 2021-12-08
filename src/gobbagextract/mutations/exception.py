class NothingToDo(Exception):
    @classmethod
    def file_not_available(cls, fname: str):
        return cls(f"File '{fname}' not (yet) available for download.")
