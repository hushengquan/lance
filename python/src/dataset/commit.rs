// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::sync::LazyLock;

use lance_table::io::commit::external_manifest::ExternalManifestStore;
use lance_table::io::commit::{CommitError, CommitLease, CommitLock};
use snafu::location;

use lance_core::Error;

use pyo3::{exceptions::PyIOError, prelude::*};

static PY_CONFLICT_ERROR: LazyLock<PyResult<Py<PyAny>>> = LazyLock::new(|| {
    Python::attach(|py| {
        py.import("lance")
            .and_then(|lance| lance.getattr("commit"))
            .and_then(|commit| commit.getattr("CommitConflictError"))
            .map(|err| err.unbind())
    })
});

fn handle_error(py_err: PyErr, py: Python) -> CommitError {
    let conflict_err_type = match &*PY_CONFLICT_ERROR {
        Ok(err) => err.bind(py).get_type(),
        Err(import_error) => {
            return CommitError::OtherError(Error::Internal {
                message: format!("Error importing from pylance {}", import_error),
                location: location!(),
            })
        }
    };

    if py_err.is_instance(py, &conflict_err_type) {
        CommitError::CommitConflict
    } else {
        CommitError::OtherError(Error::Internal {
            message: format!("Error from commit handler: {}", py_err),
            location: location!(),
        })
    }
}

pub struct PyCommitLock {
    inner: Py<PyAny>,
}

impl PyCommitLock {
    pub fn new(inner: Py<PyAny>) -> Self {
        Self { inner }
    }
}

impl Debug for PyCommitLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = Python::attach(|py| {
            self.inner
                .call_method0(py, "__repr__")?
                .extract::<String>(py)
        })
        .ok();
        f.debug_struct("PyCommitLock")
            .field("inner", &repr)
            .finish()
    }
}

#[async_trait::async_trait]
impl CommitLock for PyCommitLock {
    type Lease = PyCommitLease;

    async fn lock(&self, version: u64) -> Result<Self::Lease, CommitError> {
        let lease = Python::attach(|py| -> Result<_, CommitError> {
            let lease = self
                .inner
                .call1(py, (version,))
                .map_err(|err| handle_error(err, py))?;
            lease
                .call_method0(py, "__enter__")
                .map_err(|err| handle_error(err, py))?;
            Ok(lease)
        })?;
        Ok(PyCommitLease { inner: lease })
    }
}

pub struct PyCommitLease {
    inner: Py<PyAny>,
}

#[async_trait::async_trait]
impl CommitLease for PyCommitLease {
    async fn release(&self, success: bool) -> Result<(), CommitError> {
        Python::attach(|py| {
            if success {
                self.inner
                    .call_method1(py, "__exit__", (py.None(), py.None(), py.None()))
                    .map_err(|err| handle_error(err, py))
            } else {
                // If the commit failed, we pass up an exception to the
                // context manager.
                PyIOError::new_err("commit failed").restore(py);
                let args = py
                    .import("sys")
                    .unwrap()
                    .getattr("exc_info")
                    .unwrap()
                    .call0()
                    .unwrap();
                self.inner
                    .call_method1(
                        py,
                        "__exit__",
                        (
                            args.get_item(0).unwrap(),
                            args.get_item(1).unwrap(),
                            args.get_item(2).unwrap(),
                        ),
                    )
                    .map_err(|err| handle_error(err, py))
            }
        })?;
        Ok(())
    }
}

/// Python-side ExternalManifestStore that bridges to Rust's ExternalManifestStore trait.
/// This allows business logic to implement ExternalStore in Python and use it from both
/// Python and Rust APIs, ensuring atomic commits across both languages.
pub struct PyExternalManifestStore {
    inner: Py<PyAny>,
}

impl PyExternalManifestStore {
    pub fn new(inner: Py<PyAny>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for PyExternalManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = Python::attach(|py| {
            self.inner
                .call_method0(py, "__repr__")?
                .extract::<String>(py)
        })
        .ok();
        f.debug_struct("PyExternalManifestStore")
            .field("inner", &repr)
            .finish()
    }
}

#[async_trait::async_trait]
impl ExternalManifestStore for PyExternalManifestStore {
    async fn get(&self, base_uri: &str, version: u64) -> Result<String, Error> {
        Python::attach(|py| -> Result<String, Error> {
            let result = self
                .inner
                .call_method1(py, "get", (base_uri, version))
                .map_err(|err| {
                    if err.is_instance_of::<pyo3::exceptions::PyKeyError>(py) {
                        Error::NotFound {
                            uri: format!("{}@{}", base_uri, version),
                            location: location!(),
                        }
                    } else {
                        Error::Internal {
                            message: format!("Error from external store get: {}", err),
                            location: location!(),
                        }
                    }
                })?;
            result.extract::<String>(py).map_err(|err| Error::Internal {
                message: format!("Failed to extract string from get result: {}", err),
                location: location!(),
            })
        })
    }

    async fn get_latest_version(&self, base_uri: &str) -> Result<Option<(u64, String)>, Error> {
        Python::attach(|py| -> Result<Option<(u64, String)>, Error> {
            let result = self
                .inner
                .call_method1(py, "get_latest_version", (base_uri,))
                .map_err(|err| Error::Internal {
                    message: format!("Error from external store get_latest_version: {}", err),
                    location: location!(),
                })?;

            if result.is_none(py) {
                return Ok(None);
            }

            let tuple: (u64, String) = result.extract(py).map_err(|err| Error::Internal {
                message: format!(
                    "Failed to extract (version, path) tuple from get_latest_version result: {}",
                    err
                ),
                location: location!(),
            })?;

            Ok(Some(tuple))
        })
    }

    async fn put_if_not_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        size: u64,
        e_tag: Option<String>,
    ) -> Result<(), Error> {
        Python::attach(|py| -> Result<(), Error> {
            let e_tag_py = match e_tag {
                Some(ref tag) => tag.into_pyobject(py).unwrap().into_any().unbind(),
                None => py.None(),
            };

            self.inner
                .call_method1(
                    py,
                    "put_if_not_exists",
                    (base_uri, version, path, size, e_tag_py),
                )
                .map_err(|err| Error::from(handle_error(err, py)))?;

            Ok(())
        })
    }

    async fn put_if_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        size: u64,
        e_tag: Option<String>,
    ) -> Result<(), Error> {
        Python::attach(|py| -> Result<(), Error> {
            let e_tag_py = match e_tag {
                Some(ref tag) => tag.into_pyobject(py).unwrap().into_any().unbind(),
                None => py.None(),
            };

            self.inner
                .call_method1(
                    py,
                    "put_if_exists",
                    (base_uri, version, path, size, e_tag_py),
                )
                .map_err(|err| Error::from(handle_error(err, py)))?;

            Ok(())
        })
    }

    async fn delete(&self, base_uri: &str) -> Result<(), Error> {
        Python::attach(|py| -> Result<(), Error> {
            self.inner
                .call_method1(py, "delete", (base_uri,))
                .map_err(|err| Error::Internal {
                    message: format!("Error from external store delete: {}", err),
                    location: location!(),
                })?;

            Ok(())
        })
    }
}
