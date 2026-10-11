namespace SingleTenancyRepro;

public record Counted(int Amount);

public class Tally
{
    public Guid Id { get; set; }
    public int Total { get; set; }

    public void Apply(Counted e) => Total += e.Amount;
}
