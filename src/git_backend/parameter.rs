use pgwire::api::portal::Portal;

pub fn make_qeury(portal: &Portal<String>) -> String {
    let mut result_query = String::new();
    let input_query = &portal.statement.statement;

    for token in input_query.split(' ') {
        if let Some(first) = token.chars().nth(0) {
            match first {
                '$' => {
                    let i = &token[1..].parse::<usize>().unwrap();
                    match get_param(portal, *i - 1) {
                        Some(param) => result_query.push_str(&param),
                        None => result_query.push_str(""),
                    }
                }
                _ => result_query.push_str(token),
            }

            result_query.push_str(" ")
        }
    }

    result_query
}

pub fn get_param(portal: &Portal<String>, i: usize) -> Option<String> {
    let param_type = portal.statement.parameter_types.get(i).unwrap();
    portal.parameter::<String>(i, param_type).unwrap()
}
