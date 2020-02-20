#[macro_export]
/// Reader Macro
/// Use this to easily write into a given datastructure in State.
macro_rules! make_reader {
    ($id:ident, $name:ident) => {
        macro_rules! $name {
            ($state:expr) => {
                &$state.$id
            };
            ($state:expr, $key:expr) => {
                $state.$id.get($key)
            };
        }
    };
}

#[macro_export]
/// Writer Macro
/// Use this to easily write into a given datastructure in State.
macro_rules! make_writer {
    ($id:ident, $name:ident) => {
        macro_rules! $name {
            ($state:expr) => {
                &$state.$id
            };
            ($state:expr, $key:expr) => {
                $state.$id.get_mut($key)
            };
        }
    };
}

#[macro_export]
/// Macro to generate the enum AND store each variant in OP_VARIANTS
macro_rules! op_variants {
    ($name:ident, $($variant_name:ident($($arg:ty),*)),*) => {
        lazy_static! {
            pub static ref OP_VARIANTS: Vec<String> = {
                let mut v = Vec::new();
                v.push(format!("{}", stringify!($name)));
                $(
                    v.push(format!("{}", stringify!($variant_name($($arg),*))));
                )*
                v
            };
        }
        crate::as_item! {
            #[derive(Debug, Clone)]
            pub enum $name { $($variant_name($($arg),*),)* }
        }
    };
}

#[macro_export]
macro_rules! as_item {
    ($i:item) => {
        $i
    };
}
