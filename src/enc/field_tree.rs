use super::Result;
use itertools::Itertools;
use snafu::prelude::*;
use std::fmt;

#[derive(Debug)]
pub enum FieldTree<E> {
    Empty,
    Node {
        tag: String,
        children: Vec<FieldTree<E>>,
        extra: E,
    },
    Leaf {
        tag: String,
        extra: E,
    },
}
impl FieldTree<()> {
    fn insert(&mut self, parent: &str, child: &str) -> bool {
        match self {
            Self::Empty => {
                *self = FieldTree::Node {
                    tag: parent.to_string(),
                    children: vec![FieldTree::Leaf {
                        tag: child.to_string(),
                        extra: (),
                    }],
                    extra: (),
                };
                return true;
            }
            Self::Node {
                tag,
                children,
                extra: _,
            } => {
                if *tag == parent {
                    children.push(Self::Leaf {
                        tag: child.to_string(),
                        extra: (),
                    });
                    return true;
                } else {
                    for c in children {
                        if c.insert(parent, child) {
                            return true;
                        }
                    }
                    // Not in this sub-tree;
                    return false;
                }
            }
            Self::Leaf { tag, extra } => {
                if *tag == parent {
                    let mut new_parent_node = Self::Node {
                        tag: tag.clone(),
                        children: vec![Self::Leaf {
                            tag: child.to_string(),
                            extra: (),
                        }],
                        extra: extra.clone(),
                    };
                    std::mem::swap(self, &mut new_parent_node);
                    return true;
                } else {
                    // Not in this sub-tree.
                    return false;
                }
            }
        };
    }

    #[allow(unused)]
    pub fn parse_from_str(input: &str, tag_length: usize) -> Result<Self> {
        let mut remaining: &str = input;
        let mut tree: Self = Self::Empty;
        while !remaining.is_empty() {
            let (parent, rest) = remaining.split_at(tag_length);
            let (child, rest) = rest.split_at(tag_length);
            remaining = rest;
            ensure_whatever!(
                tree.insert(parent, child),
                "Could not insert {parent} -> {child} into {tree:#?}"
            );
        }
        Ok(tree)
    }
}
impl<E> FieldTree<E>
where
    E: fmt::Display,
{
    pub fn tree_string(&self) -> String {
        self.tree_string_prefixed("".to_string())
    }

    fn tree_string_prefixed(&self, prefix: String) -> String {
        match self {
            Self::Empty => "|".to_string(),
            Self::Leaf { tag, extra } => format!("{prefix}|- {tag}: {extra}"),
            Self::Node {
                tag,
                children,
                extra,
            } => {
                let child_prefix = prefix.clone() + "   ";
                format!(
                    "{prefix}|-{tag}: {extra}\n{}",
                    children
                        .iter()
                        .map(move |child| child.tree_string_prefixed(child_prefix.clone()))
                        .join("\n")
                )
            }
        }
    }
}
impl<E> FieldTree<E> {
    pub fn map_extra<O, F>(self, mapper: F) -> FieldTree<O>
    where
        F: Fn(&str) -> O + Copy,
    {
        match self {
            Self::Empty => FieldTree::Empty,
            Self::Leaf { tag, .. } => {
                let extra = mapper(&tag);
                FieldTree::Leaf { tag, extra }
            }
            Self::Node { tag, children, .. } => {
                let extra = mapper(&tag);
                let new_children = children
                    .into_iter()
                    .map(|child| child.map_extra(mapper))
                    .collect();
                FieldTree::Node {
                    tag,
                    children: new_children,
                    extra,
                }
            }
        }
    }
}
impl<E> fmt::Display for FieldTree<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.tree_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enc::reader::Generic8211FileReader;

    const TEST_CHART_FILE: &str = "/Users/lkroll/Programming/Sailing/test-data/20240816_U7Inland_Waddenzee_week 33_NL/ENC_ROOT/1R/7/1R7EMS01/1R7EMS01.000";

    #[test]
    fn read_field_tree() {
        let test_str = "0001DSIDDSIDDSSI0001DSPM0001VRIDVRIDATTVVRIDVRPCVRIDVRPTVRIDSGCCVRIDSG2DVRIDSG3D0001FRIDFRIDFOIDFRIDATTFFRIDNATFFRIDFFPCFRIDFFPTFRIDFSPCFRIDFSPT";
        let field_tree = FieldTree::parse_from_str(test_str, 4).unwrap();
        println!("{:#?}", field_tree);
        let field_tree_with_sub_fields = field_tree.map_extra(|tag| tag.to_string());
        println!("{}", field_tree_with_sub_fields);

        let generic_reader = Generic8211FileReader::open(TEST_CHART_FILE).unwrap();
        let tag_tree = FieldTree::parse_from_str(
            &generic_reader.ddr_record.control_field.field_tag_pairs,
            generic_reader.ddr_record.leader.size_of_field_tag as usize,
        )
        .unwrap();
        println!("{:#?}", tag_tree);
        let tag_tree_with_sub_fields = tag_tree.map_extra(|tag| {
            if let Some(descriptor) = generic_reader.ddr_record.get_descriptor_with_tag(tag) {
                format!(
                    "{} {}",
                    descriptor.array_descriptor, descriptor.format_controls
                )
            } else {
                "<descriptor not found>".to_string()
            }
        });
        println!("{}", tag_tree_with_sub_fields);
    }
}
