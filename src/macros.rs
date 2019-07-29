#[macro_export]
macro_rules! make_reader {
    ($id:ident) => {
        macro_rules! read_$id {
            ($state:expr) => {
                $state.$id.read()
            };
            ($state:expr, $key:expr) => {
                $state.$id.read().get($key)
            };
            ($state:expr, $key:expr, $var_name:ident) => {
                let __temp_name = $state.$id.read();
                let $var_name = __temp_name.get($key);
            };
        }
    };
}

macro_rules! make_writer {
    ($id:ident) => {
        macro_rules! write_$id {
            ($state:expr) => {
                $state.$id.write()
            };
            ($state:expr, $key:expr) => {
                $state.$id.write().get_mut($key)
            };
            ($state: expr, $key:expr, $var_name:ident) => {
                let mut __temp_name = $state.$id.write();
                let $var_name = __temp_name.get_mut($key).unwrap();
            };
        }
    };
}
// macro_rules! write_$id {
//     ($state:expr) => {
//         $state.$id.write()
//     };
//     ($state:expr, $key:expr) => {
//         $state.$id.write().get_mut($key)
//     };
//     ($state: expr, $key:expr, $var_name:ident) => {
//         let mut __temp_name = $state.$id.write();
//         let $var_name = __temp_name.get_mut($key).unwrap();
// };
// }
// macro_rules! read_sets {
//     ($state:expr) => {
//         $state.$id.read()
//     };
//     ($state:expr, $key:expr) => {
//         $state.$id.read().get($key)
//     };
//     ($state:expr, $key:expr, $var_name:ident) => {
//         let __temp_name = $state.$id.read();
//         let $var_name = __temp_name.get($key);
//     };
// }

// macro_rules! write_sets {
//     ($state:expr) => {
//         $state.$id.write()
//     };
//     ($state:expr, $key:expr) => {
//         $state.$id.write().get_mut($key)
//     };
//     ($state: expr, $key:expr, $var_name:ident) => {
//         let mut __temp_name = $state.$id.write();
//         let $var_name = __temp_name.get_mut($key).unwrap();
//     };
// }
