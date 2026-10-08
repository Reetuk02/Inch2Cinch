namespace ProductService.Data
{
    public class Product
    {
        public Guid Id { get; set; }

        public string ProductCode { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public decimal Price { get; set; }

        public DateTime UpdatedAtUtc { get; set; }
    }
}
