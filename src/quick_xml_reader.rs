use super::*;

use quick_xml::{
    events::{BytesStart, Event},
    Reader,
};

use crate::osm_data::*;

pub fn scan_nodes<P, F, T>(path: P, mut scanner: F, initial: T) -> Result<T, ScanError>
where
    P: AsRef<Path>,
    F: FnMut(Node, T) -> T,
{
    let mut parser = Reader::from_file(path)?;
    parser.trim_text(true);
    let mut buffer: Vec<u8> = Vec::with_capacity(BUFFER_SIZE / 2);

    let mut acc = initial;
    loop {
        let event = parser.read_event(&mut buffer)?;
        match event {
            Event::Start(ref e) => match e.name() {
                b"osm" => {
                    acc = scan_osm(&mut parser, &mut scanner, acc, &mut buffer)?;
                }
                _ => {
                    return Err(ScanError::Other(format!("An unexpected start element: {:?}", e.name())));
                }
            },
            Event::Eof => return Ok(acc),
            Event::Decl(_) => continue,
            _ => {
                return Err(ScanError::Other(format!("An unexpected xml event: {:?}", event)));
            }
        }
        buffer.clear();
    }
}

fn scan_osm<R, F, T>(parser: &mut Reader<R>, scanner: &mut F, initial: T, buffer: &mut Vec<u8>) -> Result<T, ScanError>
where
    R: std::io::BufRead,
    F: FnMut(Node, T) -> T,
{
    let mut acc = initial;
    let mut inner_buffer: Vec<u8> = Vec::with_capacity(BUFFER_SIZE / 2);
    loop {
        let event = parser.read_event(buffer)?;
        match event {
            Event::Start(ref e) => match e.name() {
                b"node" => {
                    let node = read_node(parser, e, &mut inner_buffer, false)?;
                    inner_buffer.clear();
                    acc = scanner(node, acc);
                }
                b"note" | b"way" | b"relation" => {
                    parser.read_to_end(e.name(), &mut inner_buffer)?;
                    inner_buffer.clear();
                }
                _ => {
                    return Err(ScanError::Other(format!(
                        "An unexpected start element: {:?}",
                        from_utf8(e.name())
                    )));
                }
            },
            Event::Empty(ref e) => match e.name() {
                b"node" => {
                    let node = read_node(parser, e, &mut inner_buffer, true)?;
                    acc = scanner(node, acc);
                }
                b"meta" => continue,
                _ => {
                    return Err(ScanError::Other(format!(
                        "An unexpected empty element: {:?}",
                        from_utf8(e.name())
                    )));
                }
            },
            Event::End(ref e) => {
                if e.name() == b"osm" {
                    return Ok(acc);
                } else {
                    return Err(ScanError::Other(format!(
                        "Unexpected end element: {:?}",
                        from_utf8(e.name())
                    )));
                }
            }
            _ => {
                return Err(ScanError::Other(format!("An unexpected xml event: {:?}", event)));
            }
        }
        buffer.clear();
    }
}

fn read_node<'e, R: std::io::BufRead>(
    parser: &mut Reader<R>,
    event: &BytesStart<'e>,
    buffer: &mut Vec<u8>,
    skip_content: bool,
) -> Result<Node, ScanError> {
    let mut node = Node::uninitialised();
    for a_res in event.attributes() {
        let a = a_res?;
        match a.key {
            b"id" => {
                let value = a.unescape_and_decode_value(parser)?;
                let id: i64 = value.parse()?;
                node.id = id;
            }
            b"lat" => {
                let value = a.unescape_and_decode_value(parser)?;
                let lat: Latitude = value.parse()?;
                node.lat = lat;
            }
            b"lon" => {
                let value = a.unescape_and_decode_value(parser)?;
                let lon: Longitude = value.parse()?;
                node.lon = lon;
            }
            _ => (), // ignore other fields
        }
    }
    if skip_content {
        return Ok(node);
    }
    loop {
        let event = parser.read_event(buffer)?;
        match event {
            Event::Start(ref e) => {
                return Err(ScanError::Other(format!(
                    "An unexpected start element: {:#?}",
                    from_utf8(e.name())
                )));
                //panic!("A none-tag start element: {:?}", name);
            }
            Event::Empty(ref e) => {
                if e.name() == b"tag" {
                    let tag = read_tag(parser, e)?;
                    node.tags.push(tag);
                } else {
                    return Err(ScanError::Other(format!(
                        "A none-tag empty element: {:?}",
                        from_utf8(e.name())
                    )));
                    //panic!("A none-tag start element: {:?}", name);
                }
            }
            Event::End(ref e) => match e.name() {
                b"node" => return Ok(node),
                _ => {
                    return Err(ScanError::Other(format!(
                        "A none-node end element: {:?}",
                        from_utf8(e.name())
                    )));
                    //panic!("A none-node end element: {:?}", name)
                }
            },
            _ => {
                return Err(ScanError::Other(format!("An unexpected xml event: {:?}", event)));
                //panic!("An unexpected xml event: {:?}", name);
            }
        }
    }
}

fn read_tag<'e, R: std::io::BufRead>(parser: &mut Reader<R>, event: &BytesStart<'e>) -> Result<Tag, ScanError> {
    let mut key_opt: Option<String> = None;
    let mut value_opt: Option<String> = None;
    for a_res in event.attributes() {
        let a = a_res?;
        match a.key {
            b"k" => {
                let value = a.unescape_and_decode_value(parser)?;
                key_opt = Some(value);
            }
            b"v" => {
                let value = a.unescape_and_decode_value(parser)?;
                value_opt = Some(value);
            }
            _ => (), // ignore other fields
        }
    }
    let key = key_opt.ok_or_else(|| ScanError::Other("tag has no key".into()))?;
    let value = value_opt.ok_or_else(|| ScanError::Other("tag has no value".into()))?;
    Ok(Tag { key, value })
}

// fn skip_element<R: std::io::Read>(
//     parser: &mut EventReader<R>,
//     element_name: OwnedName,
// ) -> Result<(), ScanError> {
//     loop {
//         let event = parser.next()?;
//         match event {
//             XmlEvent::EndElement { name } => {
//                 if name.local_name == element_name.local_name {
//                     return Ok(());
//                 }
//                 // else {
//                 //     panic!(
//                 //         "Names didn't match: {} != {}",
//                 //         name.local_name, element_name.local_name
//                 //     );
//                 // }
//             }
//             XmlEvent::EndDocument => {
//                 return Err(ScanError::Other(format!(
//                     "Document ended while skipping element: {:?}",
//                     element_name
//                 )))
//             }
//             _ => continue,
//         }
//     }
// }

fn from_utf8(slice: &[u8]) -> &str {
    std::str::from_utf8(slice).expect("Could not read slice as Utf8")
}
