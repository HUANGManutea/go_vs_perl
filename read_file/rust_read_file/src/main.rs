use std::fs::File;
use std::io::{self, BufRead};
use std::collections::{HashMap};
use std::str;
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};

#[derive(Serialize, Deserialize)]
struct FieldFormat {
	Offset: i32,
	Length: i32
}

#[derive(Serialize, Deserialize)]
struct Format {
	LgRecord: i32,
	Fields:   HashMap::<String, *mut FieldFormat>
}

fn main() {
	format = read_format();
	read_file(format);
}

fn print_result(data: HashMap::<String, String>) -> String {
	let res = serde_json::to_string(&data)?;
	if err != nil {
		panic!("Cannot marshal format !");
	}
	return res
}

fn read_file(format: Format) {
    let f = File::open("../data2.txt").expect("Unable to open file ../data2.txt");
    let result_lines = io::BufReader::new(f).lines();

    for result_line in result_lines {
        if let Ok(line) = result_line {
            let result = parse_line(format, line);
            let json_format = print_result(result);

            println!("{}", json_format)
        }
    }
}

fn parse_line(format: Format, line: String) -> HashMap::<String, String> {
	let result = HashMap::<String, String>::new();
	for (k, v) in format.Fields {
        let start = (&*v).Offset as usize;
        let end = ((&*v).Offset+(&*v).Length) as usize;
		result[(&*k)] = String::from(&line[start..end]);
	}
	return result
}

fn read_format() -> Format {
    let f = File::open("../format.txt").expect("Unable to open file ../format.txt");
    let result_lines = io::BufReader::new(f).lines();

    
	// {
	// 	field_name: {
	// 		offset: "",
	// 		length: "",
	//      value: ""
	// 	}
	// }
	let field_map = HashMap::<String, *mut FieldFormat>::new();
	let temp_lgRecord = 0;


    for result_line in result_lines {
        if let Ok(line) = result_line {
            let field_data = line.split(" ").collect::<Vec<&str>>();
            if line.starts_with("LGRECORD") {
                let lgRecord = match field_data[1].parse::<i32>() {
                    Ok(lgRecord) => lgRecord,
                    Err(_e) => panic!("erreur format LgRecord: {}", field_data[1]),
                  };
                temp_lgRecord = lgRecord
            } else {
                if field_data.len() != 3 {
                    panic!("length field_data != 3, line: {}", line)
                }
                let offset = match field_data[1].parse::<i32>() {
                    Ok(offset) => offset,
                    Err(_e) => panic!("erreur format offset: {}", field_data[1]),
                };

                let length = match field_data[2].parse::<i32>() {
                    Ok(offset) => offset,
                    Err(_e) => panic!("erreur format length: {}", field_data[2]),
                };
                field_map[field_data[0]] = &mut FieldFormat{Offset: offset, Length: length}
            }
        }
    }

	let format = Format{LgRecord: temp_lgRecord, Fields: field_map};

	return format
}
