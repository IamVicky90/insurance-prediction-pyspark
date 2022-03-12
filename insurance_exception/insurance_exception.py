import os
class InsuranceException(Exception):
    def __init__(self,eror_message,sys)-> str:
        """Error Handling Layer for Insurance Project

        Args:
            eror_message str: [description]
            sys : For Error Details

        Returns:
            str: Overall Error Message.
        """
        exc_type, _, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        self.error= f'Error Message {eror_message}, Error Type {exc_type} filename: {fname}, Line Number: {exc_tb.tb_lineno}'
    def __repr__(self):
        return InsuranceException.__name__.__str__()
    def __str__(self):
        return self.error
    