fn main() {

    struct self_ref<'this>{
        num :i32,
        num_ref:&'this i32
    }


}