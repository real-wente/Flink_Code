package Test.Table;

/**
 * @author wentao
 * @date 2022-04-13  17:29
 */

public class MyOrder {
    public Long id;
    public String product;
    public int amount;

    public MyOrder(Long id,String product,int amount){
        this.id=id;
        this.product=product;
        this.amount=amount;
    }

    @Override
    public String toString() {
        return "MyOrder{"+
                "id=" +id +
                ",product='" + product +'\''+
                ",amount=" + amount +
                '}';
    }
}
