class CommentFile(object):
    """Comment File"""

    def __init__(self, file_handle, commentstring="#"):
        self.file_handle = file_handle
        self.commentstring = commentstring

    def next(self):
        """next"""

        line = self.file_handle.next()
        while line.startswith(self.commentstring):
            line = self.file_handle.next()
        return line

    def __iter__(self):
        return self
