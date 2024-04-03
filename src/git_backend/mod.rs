use std::collections::HashMap;
use std::fs;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use gitql_ast::object::GitQLObject;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, MakeHandler};
use pgwire::error::{PgWireError, PgWireResult};

use git_data_provider::GitDataProvider;
use git_schema::TABLES_FIELDS_NAMES;
use git_schema::TABLES_FIELDS_TYPES;
use gitql_ast::environment::Environment;
use gitql_ast::schema::Schema;
use gitql_engine::data_provider::DataProvider;
use gitql_engine::engine::{self, EvaluationResult::SelectedGroups};
use gitql_parser::parser;
use gitql_parser::tokenizer;

use git_column::encode_column;
use git_row::encode_row;

mod git_column;
mod git_data_provider;
mod git_row;
mod git_schema;

pub struct GitQLBackend {
    repositories: Arc<[String]>,
    query_parser: Arc<NoopQueryParser>,
    query_cache: Arc<Mutex<HashMap<String, GitQLObject>>>,
}

#[async_trait]
impl SimpleQueryHandler for GitQLBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let git_repo_result = validate_git_repositories(&self.repositories);
        if query.to_uppercase().starts_with("DEALLOCATE") {
            return Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))]);
        }

        if git_repo_result.is_err() {
            println!("Failed to load git repositories");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                git_repo_result.err().unwrap(),
            )));
        }

        let repos = git_repo_result.ok().unwrap();
        let schema = Schema {
            tables_fields_names: TABLES_FIELDS_NAMES.to_owned(),
            tables_fields_types: TABLES_FIELDS_TYPES.to_owned(),
        };

        let mut env = Environment::new(schema);
        let query = query.split(';').next().unwrap();
        let tokenizer_result = tokenizer::tokenize(query.to_string());
        if tokenizer_result.is_err() {
            println!("Cannot tokenize result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                tokenizer_result.err().unwrap().message().to_owned(),
            )));
        }

        let tokens = tokenizer_result.ok().unwrap();
        if tokens.is_empty() {
            println!("Empty Tokens");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Empty Tokens",
            )));
        }

        let parser_result = parser::parse_gql(tokens, &mut env);
        if parser_result.is_err() {
            let parser_err = parser_result.err().unwrap();
            let error_message =
                parser_err.message().to_owned() + "\nHelp: " + &parser_err.helps().join("\n");
            println!("Cannot parse result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                error_message,
            )));
        }

        let query_node = parser_result.ok().unwrap();

        let provider: Box<dyn DataProvider> = Box::new(GitDataProvider::new(repos.to_vec()));
        let evaluation_result = engine::evaluate(&mut env, &provider, query_node);

        if evaluation_result.is_err() {
            println!("Cannot evaluate result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                evaluation_result.err().unwrap(),
            )));
        }
        let engine_result = evaluation_result.ok().unwrap();

        if let SelectedGroups(mut groups, hidden_selection) = engine_result {
            let mut indexes = vec![];
            for (index, title) in groups.titles.iter().enumerate() {
                if hidden_selection.contains(title) {
                    indexes.insert(0, index);
                }
            }

            if groups.len() > 1 {
                groups.flat();
            }

            for index in indexes {
                groups.titles.remove(index);

                for row in &mut groups.groups[0].rows {
                    row.values.remove(index);
                }
            }

            let mut fields_info: Vec<FieldInfo> = vec![];

            for (index, title) in groups.titles.iter().enumerate() {
                let field_result = encode_column(title, index);
                if field_result.is_err() {
                    continue;
                }
                fields_info.push(field_result.ok().unwrap());
            }

            let result = encode_row(&groups, Arc::new(fields_info.clone()));
            return Ok(vec![Response::Query(QueryResponse::new(
                Arc::new(fields_info),
                result,
            ))]);
        }

        Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
    }
}

#[async_trait]
impl ExtendedQueryHandler for GitQLBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        if query.to_lowercase().starts_with("set") {
            return Ok(Response::Execution(Tag::new("OK").with_rows(1)));
        }

        let locked_cache = self.query_cache.lock().unwrap();
        match locked_cache.get(query) {
            Some(groups) => {
                let mut fields_info: Vec<FieldInfo> = vec![];

                for (index, title) in groups.titles.iter().enumerate() {
                    let field_result = encode_column(title, index);
                    if field_result.is_err() {
                        continue;
                    }
                    fields_info.push(field_result.ok().unwrap());
                }

                let result = encode_row(groups, Arc::new(fields_info.clone()));
                Ok(Response::Query(QueryResponse::new(
                    Arc::new(fields_info),
                    result,
                )))
            }
            None => Ok(Response::Execution(Tag::new("OK").with_rows(1))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let param_types = stmt.parameter_types.clone();
        let statement = &stmt.statement;
        println!("Statement {:?}, {}", param_types, statement);

        return Err(PgWireError::IoError(Error::new(
            ErrorKind::Other,
            "Failed to make statement result",
        )));
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse> {
        if portal.statement.statement.to_lowercase().starts_with("set") {
            return Ok(DescribePortalResponse::new(vec![]));
        }

        let git_repo_result = validate_git_repositories(&self.repositories);
        let repos = git_repo_result.ok().unwrap();

        let schema = Schema {
            tables_fields_names: TABLES_FIELDS_NAMES.to_owned(),
            tables_fields_types: TABLES_FIELDS_TYPES.to_owned(),
        };

        let mut env = Environment::new(schema);
        let query = &portal.statement.statement.split(';').next().unwrap();
        let tokenizer_result = tokenizer::tokenize(query.to_string());
        if tokenizer_result.is_err() {
            println!("Cannot tokenize result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                tokenizer_result.err().unwrap().message().to_owned(),
            )));
        }

        let tokens = tokenizer_result.ok().unwrap();
        if tokens.is_empty() {
            println!("Empty Tokens");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                "Empty Tokens",
            )));
        }

        let parser_result = parser::parse_gql(tokens, &mut env);
        if parser_result.is_err() {
            let parser_err = parser_result.err().unwrap();
            let error_message =
                parser_err.message().to_owned() + "\nHelp: " + &parser_err.helps().join("\n");
            println!("Cannot parse result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                error_message,
            )));
        }

        let query_node = parser_result.ok().unwrap();

        let provider: Box<dyn DataProvider> = Box::new(GitDataProvider::new(repos.to_vec()));
        let evaluation_result = engine::evaluate(&mut env, &provider, query_node);

        if evaluation_result.is_err() {
            println!("Cannot evaluate result");
            return Err(PgWireError::IoError(Error::new(
                ErrorKind::Other,
                evaluation_result.err().unwrap(),
            )));
        }
        let engine_result = evaluation_result.ok().unwrap();

        if let SelectedGroups(mut groups, hidden_selection) = engine_result {
            let mut indexes = vec![];
            for (index, title) in groups.titles.iter().enumerate() {
                if hidden_selection.contains(title) {
                    indexes.insert(0, index);
                }
            }

            if groups.len() > 1 {
                groups.flat();
            }

            for index in indexes {
                groups.titles.remove(index);

                for row in &mut groups.groups[0].rows {
                    row.values.remove(index);
                }
            }

            let mut fields_info: Vec<FieldInfo> = vec![];

            for (index, title) in groups.titles.iter().enumerate() {
                let field_result = encode_column(title, index);
                if field_result.is_err() {
                    continue;
                }
                fields_info.push(field_result.ok().unwrap());
            }

            let mut locked_cache = self.query_cache.lock().unwrap();
            locked_cache.insert(query.to_string(), groups);

            return Ok(DescribePortalResponse::new(fields_info));
        }

        return Ok(DescribePortalResponse::new(vec![]));
    }
}

pub struct MakeGitQLBackend {
    repositories: Arc<[String]>,
    query_parser: Arc<NoopQueryParser>,
}

impl MakeGitQLBackend {
    pub fn new(path: Option<String>) -> MakeGitQLBackend {
        let entries = fs::read_dir(path.as_deref().unwrap_or("."))
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok().unwrap();
                let path = entry.path();
                if path.is_dir() {
                    return path.into_os_string().into_string().ok();
                }
                None
            })
            .collect::<Vec<_>>();

        MakeGitQLBackend {
            repositories: Arc::from(entries),
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

impl MakeHandler for MakeGitQLBackend {
    type Handler = Arc<GitQLBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(GitQLBackend {
            repositories: self.repositories.clone(),
            query_parser: self.query_parser.clone(),
            query_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

fn validate_git_repositories(repositories: &Arc<[String]>) -> Result<Vec<gix::Repository>, String> {
    let mut git_repositories: Vec<gix::Repository> = vec![];
    for repository in repositories.iter() {
        let git_repository = gix::open(repository);
        if git_repository.is_err() {
            println!("This is not git repository");
            continue;
        }
        git_repositories.push(git_repository.ok().unwrap());
    }
    Ok(git_repositories)
}
