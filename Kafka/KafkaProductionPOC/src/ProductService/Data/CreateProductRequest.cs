namespace ProductService.Data
{
    public record CreateProductRequest(
        string ProductCode,
        string Name,
        decimal Price);
}
