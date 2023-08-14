class Producto:

    def __init__(self):

        self.product_id = ""
        self.price = 0
        self.product = ""
        self.brand = ""
        self.unit_price = 0
        self.units = 0
        self.categories = []
        self.discount = 0

    def to_dict(self) -> dict:
        return self.__dict__
