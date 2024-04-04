use std::{convert::TryFrom, fmt, hash::{Hash, Hasher}, iter::FromIterator, ops::{Deref, DerefMut}};
use std::mem::ManuallyDrop;

use crate::Arc;

const CUTOFF: usize = 22;

type InlineData = [u8; CUTOFF];

/// A buffer that may either be inline or remote and protected
/// by an Arc
pub struct IVec {
    data: Data,
    state: State,
}

impl Default for IVec {
    fn default() -> Self {
        Self::from(&[])
    }
}

union Data {
    inline: InlineData,
    remote: ManuallyDrop<Arc<[u8]>>,
}

#[derive(Clone)]
enum State {
    Inline { len: u8 },
    Remote,
    Subslice { offset: usize, len: usize },
}

impl Hash for IVec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

const fn is_inline_candidate(length: usize) -> bool {
    length <= CUTOFF
}

impl IVec {
    #[allow(unsafe_code)]
    /// Create a subslice of this `IVec` that shares
    /// the same backing data and reference counter.
    ///
    /// # Panics
    ///
    /// Panics if `self.len() - offset >= len`.
    ///
    /// # Examples
    /// ```
    /// # use sled::IVec;
    /// let iv = IVec::from(vec![1]);
    /// let subslice = iv.subslice(0, 1);
    /// assert_eq!(&subslice, &[1]);
    /// let subslice = subslice.subslice(0, 1);
    /// assert_eq!(&subslice, &[1]);
    /// let subslice = subslice.subslice(1, 0);
    /// assert_eq!(&subslice, &[]);
    /// let subslice = subslice.subslice(0, 0);
    /// assert_eq!(&subslice, &[]);
    ///
    /// let iv2 = IVec::from(vec![1, 2, 3]);
    /// let subslice = iv2.subslice(3, 0);
    /// assert_eq!(&subslice, &[]);
    /// let subslice = iv2.subslice(2, 1);
    /// assert_eq!(&subslice, &[3]);
    /// let subslice = iv2.subslice(1, 2);
    /// assert_eq!(&subslice, &[2, 3]);
    /// let subslice = iv2.subslice(0, 3);
    /// assert_eq!(&subslice, &[1, 2, 3]);
    /// let subslice = subslice.subslice(1, 2);
    /// assert_eq!(&subslice, &[2, 3]);
    /// let subslice = subslice.subslice(1, 1);
    /// assert_eq!(&subslice, &[3]);
    /// let subslice = subslice.subslice(1, 0);
    /// assert_eq!(&subslice, &[]);
    /// ```
    pub fn subslice(&self, slice_offset: usize, len: usize) -> Self {
        assert!(self.len().checked_sub(slice_offset).unwrap() >= len);

        unsafe {
            match self.state {
                State::Remote => Self {
                    data: self.data.clone_as_remote(),
                    state: State::Subslice {
                        offset: slice_offset,
                        len,
                    },
                },
                State::Inline { .. } => {
                    // old length already checked above in assertion
                    let mut inline = InlineData::default();
                    inline[..len].copy_from_slice(
                        &self.data.inline[slice_offset..slice_offset + len],
                    );

                    Self {
                        data: Data { inline },
                        state: State::Inline { len: u8::try_from(len).unwrap() },
                    }
                }
                State::Subslice { ref offset, .. } => Self {
                    data: self.data.clone_as_remote(),
                    state: State::Subslice {
                        offset: offset + slice_offset,
                        len,
                    },
                }
            }
        }
    }

    fn inline(slice: &[u8]) -> Self {
        assert!(is_inline_candidate(slice.len()));
        let mut data = InlineData::default();
        data[..slice.len()].copy_from_slice(slice);
        Self {
            data: Data { inline: data },
            state: State::Inline { len: u8::try_from(slice.len()).unwrap() },
        }
    }

    fn remote(arc: Arc<[u8]>) -> Self {
        Self {
            data: Data { remote: ManuallyDrop::new(arc) },
            state: State::Remote,
        }
    }

    #[allow(unsafe_code)]
    fn make_mut(&mut self) {
        unsafe {
            match self.state {
                State::Remote if self.data.strong_count() != 1 => {
                    self.data = Data {
                        remote: ManuallyDrop::new(self.data.remote.to_vec().into()),
                    };
                }
                State::Subslice { offset, len } if self.data.strong_count() != 1 => {
                    self.data = Data {
                        remote: ManuallyDrop::new(self.data.remote[offset..offset + len].to_vec().into()),
                    };
                    self.state = State::Remote;
                }
                _ => {}
            }
        }
    }
}

impl Clone for IVec {
    #[allow(unsafe_code)]
    fn clone(&self) -> Self {
        let data = unsafe {
            match self.state {
                State::Inline { .. } => self.data.clone_as_inline(),
                State::Remote => self.data.clone_as_remote(),
                State::Subslice { .. } => self.data.clone_as_remote(),
            }
        };
        Self {
            data,
            state: self.state.clone(),
        }
    }
}

#[allow(unsafe_code)]
impl Data {
    unsafe fn clone_as_inline(&self) -> Self {
        Self { inline: self.inline }
    }

    unsafe fn clone_as_remote(&self) -> Self {
        Self { remote: self.remote.clone() }
    }

    unsafe fn strong_count(&self) -> usize {
        Arc::strong_count(&self.remote)
    }
}

impl FromIterator<u8> for IVec {
    fn from_iter<T>(iter: T) -> Self
        where
            T: IntoIterator<Item=u8>,
    {
        let bs: Vec<u8> = iter.into_iter().collect();
        bs.into()
    }
}

impl From<Box<[u8]>> for IVec {
    fn from(b: Box<[u8]>) -> Self {
        if is_inline_candidate(b.len()) {
            Self::inline(&b)
        } else {
            Self::remote(Arc::from(b))
        }
    }
}

impl From<&[u8]> for IVec {
    fn from(slice: &[u8]) -> Self {
        if is_inline_candidate(slice.len()) {
            Self::inline(slice)
        } else {
            Self::remote(Arc::from(slice))
        }
    }
}

impl From<Arc<[u8]>> for IVec {
    fn from(arc: Arc<[u8]>) -> Self {
        if is_inline_candidate(arc.len()) {
            Self::inline(&arc)
        } else {
            Self::remote(arc)
        }
    }
}

impl From<&str> for IVec {
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<&IVec> for IVec {
    fn from(v: &Self) -> Self {
        v.clone()
    }
}

impl From<Vec<u8>> for IVec {
    fn from(v: Vec<u8>) -> Self {
        if is_inline_candidate(v.len()) {
            Self::inline(&v)
        } else {
            // rely on the Arc From specialization
            // for Vec<T>, which may improve
            // over time
            Self::remote(Arc::from(v))
        }
    }
}

impl std::borrow::Borrow<[u8]> for IVec {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl std::borrow::Borrow<[u8]> for &IVec {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

macro_rules! from_array {
    ($($s:expr),*) => {
        $(
            impl From<&[u8; $s]> for IVec {
                fn from(v: &[u8; $s]) -> Self {
                    Self::from(&v[..])
                }
            }
        )*
    }
}

from_array!(
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32
);

impl Into<Arc<[u8]>> for IVec {
    #[allow(unsafe_code)]
    fn into(self) -> Arc<[u8]> {
        match self.state {
            State::Inline { .. } => Arc::from(self.as_ref()),
            State::Remote => unsafe { ManuallyDrop::into_inner(self.data.remote.clone()) },
            State::Subslice { .. } => self.deref().into(),
        }
    }
}

impl Deref for IVec {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for IVec {
    #[inline]
    #[allow(unsafe_code)]
    fn as_ref(&self) -> &[u8] {
        unsafe {
            match self.state {
                State::Inline { len } => {
                    self.data.inline.get_unchecked(..len as usize)
                },
                State::Remote => &self.data.remote,
                State::Subslice { offset, len } => {
                    &self.data.remote[offset..offset + len]
                }
            }
        }
    }
}

impl DerefMut for IVec {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl AsMut<[u8]> for IVec {
    #[inline]
    #[allow(unsafe_code)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.make_mut();

        unsafe {
            match self.state {
                State::Inline { len } => {
                    std::slice::from_raw_parts_mut(self.data.inline.as_mut_ptr(), len as usize)
                },
                State::Remote => Arc::get_mut(&mut self.data.remote).unwrap(),
                State::Subslice { offset, len } => {
                    &mut Arc::get_mut(&mut self.data.remote).unwrap()[offset..offset + len]
                }
            }
        }
    }
}

impl Ord for IVec {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for IVec {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: AsRef<[u8]>> PartialEq<T> for IVec {
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<[u8]> for IVec {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl Eq for IVec {}

impl fmt::Debug for IVec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Drop for IVec {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        match self.state {
            State::Remote | State::Subslice { .. } => {
                unsafe { ManuallyDrop::drop(&mut self.data.remote) };
            }
            _ => {}
        }
    }
}

#[test]
fn ivec_usage() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    assert_eq!(iv1, vec![1, 2, 3]);
    let iv2 = IVec::from(&[4; 128][..]);
    assert_eq!(iv2, vec![4; 128]);
}

#[test]
fn boxed_slice_conversion() {
    let boite1: Box<[u8]> = Box::new([1, 2, 3]);
    let iv1: IVec = boite1.into();
    assert_eq!(iv1, vec![1, 2, 3]);
    let boite2: Box<[u8]> = Box::new([4; 128]);
    let iv2: IVec = boite2.into();
    assert_eq!(iv2, vec![4; 128]);
}

#[test]
#[should_panic]
fn subslice_usage_00() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    let _subslice = iv1.subslice(0, 4);
}

#[test]
#[should_panic]
fn subslice_usage_01() {
    let iv1 = IVec::from(vec![1, 2, 3]);
    let _subslice = iv1.subslice(3, 1);
}

#[test]
fn ivec_as_mut_identity() {
    let initial = &[1];
    let mut iv = IVec::from(initial);
    assert_eq!(&*initial, &*iv);
    assert_eq!(&*initial, &mut *iv);
    assert_eq!(&*initial, iv.as_mut());
}

#[test]
fn ivec_alignment() {
    let iv1 = IVec::from((0..2_u64)
        .into_iter()
        .map(u64::to_be_bytes)
        .flat_map(|b| b.to_vec().into_iter())
        .collect::<Vec<u8>>());
    let kind = match iv1.state {
        State::Inline { .. } => "inline",
        State::Remote => "remote",
        State::Subslice { .. } => "subslice",
    };
    assert_eq!(iv1.as_ptr() as usize % 8, 0, "{kind}");
}

#[cfg(test)]
mod qc {
    use super::IVec;

    fn prop_identity(ivec: &IVec) -> bool {
        let mut iv2 = ivec.clone();

        if iv2 != ivec {
            println!("expected clone to equal original");
            return false;
        }

        if *ivec != *iv2 {
            println!("expected AsMut to equal original");
            return false;
        }

        if *ivec != iv2.as_mut() {
            println!("expected AsMut to equal original");
            return false;
        }

        true
    }

    quickcheck::quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn bool(item: IVec) -> bool {
            prop_identity(&item)
        }
    }
}
