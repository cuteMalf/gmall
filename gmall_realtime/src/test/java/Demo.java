public class Demo {
    public static void main(String[] args) {
        String string ="id,coupon_name,coupon_type,condition_amount,condition_num,activity_id,benefit_amount,benefit_discount,create_time,range_type,limit_num,taken_count,start_time,end_time,operate_time,expire_time,range_desc";
        String $0_varchar = string.replaceAll("[^,]+", "$0 varchar");
        System.out.println("$0_varchar = " + $0_varchar);

    }
}
