#[macro_export]
macro_rules! json_get {
    (($obj:expr) . $key:ident $($rest:tt)*) => {{
        let value = $obj.get(stringify!($key));
        json_get!(optval (value) $($rest)*)
    }};
    (($obj:expr) [ $key:expr ] $($rest:tt)*) => {{
        let value = $obj.get($key);
        json_get!(optval (value) $($rest)*)
    }};
    (value ($obj:expr) . $key:ident $($rest:tt)*) => {{
        let value = $obj.as_object().map_or(None, |v| v.get(stringify!($key)));
        json_get!(optval (value) $($rest)*)
    }};
    (value ($obj:expr) [ $key:expr ] $($rest:tt)*) => {{
        let value = $obj.as_array().map_or(None, |v| v.get($key));
        json_get!(optval (value) $($rest)*)
    }};
    (optval ($obj:expr) . $key:ident $($rest:tt)*) => {{
        let value = $obj.map_or(None, |v| v.as_object()).map_or(None, |v| v.get(stringify!($key)));
        json_get!(optval (value) $($rest)*)
    }};
    (optval ($obj:expr) [ $key:expr ] $($rest:tt)*) => {{
        let value = $obj.map_or(None, |v| v.as_array()).map_or(None, |v| v.get($key));
        json_get!(optval (value) $($rest)*)
    }};
    (optval ($obj:expr) : number) => {{
        $obj.map_or(None, |v| v.as_f64())
    }};
    (optval ($obj:expr) : string) => {{
        $obj.map_or(None, |v| v.as_str())
    }};
    (optval ($obj:expr) : object) => {{
        $obj.map_or(None, |v| v.as_object())
    }};
    (optval ($obj:expr) : array) => {{
        $obj.map_or(None, |v| v.as_array())
    }};
    (optval ($obj:expr) : u64) => {{
        $obj.map_or(None, |v| v.as_u64())
    }};
}
