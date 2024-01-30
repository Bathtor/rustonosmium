use super::*;

use crate::osm_data::*;

use xml::{
    attribute::OwnedAttribute,
    name::OwnedName,
    reader::{EventReader, XmlEvent},
    ParserConfig,
};

pub fn scan_nodes<P, F, T>(path: P, mut scanner: F, initial: T) -> Result<T, ScanError>
where
    P: AsRef<Path>,
    F: FnMut(Node, T) -> T,
{
    let file = File::open(path)?;
    let buffer = BufReader::with_capacity(BUFFER_SIZE, file);

    let mut acc = initial;
    let config = ParserConfig::default().trim_whitespace(true);
    let mut parser = config.create_reader(buffer);
    loop {
        let event = parser.next()?;
        match event {
            XmlEvent::StartElement { name, .. } => match name.local_name.as_ref() {
                "osm" => {
                    acc = scan_osm(&mut parser, &mut scanner, acc)?;
                }
                _ => {
                    return Err(ScanError::Other(format!(
                        "An unexpected start element: {:?}",
                        name
                    )));
                }
            },
            XmlEvent::StartDocument { .. } => continue,
            XmlEvent::EndDocument => return Ok(acc),
            XmlEvent::Whitespace(_) => continue,
            XmlEvent::Characters(_) => continue,
            _ => {
                return Err(ScanError::Other(format!(
                    "An unexpected xml event: {:?}",
                    event
                )));
            }
        }
    }
}

fn scan_osm<R, F, T>(
    parser: &mut EventReader<R>,
    scanner: &mut F,
    initial: T,
) -> Result<T, ScanError>
where
    R: std::io::Read,
    F: FnMut(Node, T) -> T,
{
    let mut acc = initial;
    loop {
        let event = parser.next()?;
        match event {
            XmlEvent::StartElement {
                name, attributes, ..
            } => match name.local_name.as_ref() {
                "node" => {
                    let node = read_node(parser, &attributes)?;
                    acc = scanner(node, acc);
                }
                "note" | "meta" | "way" | "relation" => skip_element(parser, name)?,
                _ => {
                    return Err(ScanError::Other(format!(
                        "An unexpected start element: {:?}",
                        name
                    )));
                }
            },
            XmlEvent::EndElement { name } => {
                if name.local_name == "osm" {
                    return Ok(acc);
                } else {
                    return Err(ScanError::Other(format!(
                        "Unexpected end element: {:?}",
                        name
                    )));
                }
            }
            XmlEvent::Whitespace(_) => continue,
            XmlEvent::Characters(_) => continue,
            _ => {
                return Err(ScanError::Other(format!(
                    "An unexpected xml event: {:?}",
                    event
                )));
            }
        }
    }
}

fn read_node<R: std::io::Read>(
    parser: &mut EventReader<R>,
    attributes: &[OwnedAttribute],
) -> Result<Node, ScanError> {
    let mut node = Node::uninitialised();
    for a in attributes {
        match a.name.local_name.as_ref() {
            "id" => {
                let id: i64 = a.value.parse()?;
                node.id = id;
            }
            "lat" => {
                let lat: Latitude = a.value.parse()?;
                node.lat = lat;
            }
            "lon" => {
                let lon: Longitude = a.value.parse()?;
                node.lon = lon;
            }
            _ => (), // ignore other fields
        }
    }
    loop {
        let event = parser.next()?;
        match event {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                if name.local_name == "tag" {
                    let tag = read_tag(attributes)?;
                    node.tags.push(tag);
                } else {
                    return Err(ScanError::Other(format!(
                        "A none-tag start element: {:?}",
                        name
                    )));
                    //panic!("A none-tag start element: {:?}", name);
                }
            }
            XmlEvent::EndElement { name } => match name.local_name.as_ref() {
                "node" => return Ok(node),
                "tag" => continue,
                _ => {
                    return Err(ScanError::Other(format!(
                        "A none-node end element: {:?}",
                        name
                    )));
                    //panic!("A none-node end element: {:?}", name)
                }
            },
            XmlEvent::Whitespace(_) => continue,
            _ => {
                return Err(ScanError::Other(format!(
                    "An unexpected xml event: {:?}",
                    event
                )));
                //panic!("An unexpected xml event: {:?}", name);
            }
        }
    }
}

fn read_tag(attributes: Vec<OwnedAttribute>) -> Result<Tag, ScanError> {
    let mut key_opt: Option<String> = None;
    let mut value_opt: Option<String> = None;
    for a in attributes {
        match a.name.local_name.as_ref() {
            "k" => {
                key_opt = Some(a.value);
            }
            "v" => {
                value_opt = Some(a.value);
            }
            _ => (), // ignore other fields
        }
    }
    let key = key_opt.ok_or_else(|| ScanError::Other("tag has no key".into()))?;
    let value = value_opt.ok_or_else(|| ScanError::Other("tag has no value".into()))?;
    Ok(Tag { key, value })
}

fn skip_element<R: std::io::Read>(
    parser: &mut EventReader<R>,
    element_name: OwnedName,
) -> Result<(), ScanError> {
    loop {
        let event = parser.next()?;
        match event {
            XmlEvent::EndElement { name } => {
                if name.local_name == element_name.local_name {
                    return Ok(());
                }
                // else {
                //     panic!(
                //         "Names didn't match: {} != {}",
                //         name.local_name, element_name.local_name
                //     );
                // }
            }
            XmlEvent::EndDocument => {
                return Err(ScanError::Other(format!(
                    "Document ended while skipping element: {:?}",
                    element_name
                )))
            }
            _ => continue,
        }
    }
}
