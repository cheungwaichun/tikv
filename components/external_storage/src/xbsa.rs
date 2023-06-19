

use std::{io::{self, Cursor}, convert::TryInto, slice, ptr, sync::{Arc, Mutex}, ffi::CString};
use futures::{io::AllowStdIo, AsyncRead, TryStreamExt};
// use futures::AsyncReadExt;
use libc::{c_int, c_ulonglong, c_long, c_float, c_uchar, c_short, c_char, c_uint, c_void};
use async_trait::async_trait;
use tikv_util::stream::error_stream;
use tokio::io::AsyncReadExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

use crate::{UnpinReader, ExternalData, ExternalStorage};
use libloading::{Library, Symbol};
use once_cell::sync::OnceCell;

#[derive(Debug, Default)]
pub struct XbsaConfig {
    pub bsa_service_host: String,
    pub bsa_policy: String,
}

/// A storage to upload file to HDFS
pub struct XbsaStorage {
    remote: Url,
    config: XbsaConfig,
}

pub const BSA_RC_SUCCESS: i32 = 0x00;
pub const BSA_RC_ABORT_SYSTEM_ERROR: i32 = 0x03;
pub const BSA_RC_ACCESS_FAILURE: i32 = 0x4D;
pub const BSA_RC_AUTHENTICATION_FAILURE: i32 = 0x04;
pub const BSA_RC_BUFFER_TOO_SMALL: i32 = 0x4E;
pub const BSA_RC_INVALID_CALL_SEQUENCE: i32 = 0x05;
pub const BSA_RC_INVALID_COPYID: i32 = 0x4F;
pub const BSA_RC_INVALID_DATABLOCK: i32 = 0x34;
pub const BSA_RC_INVALID_ENV: i32 = 0x50;
pub const BSA_RC_INVALID_HANDLE: i32 = 0x06;
pub const BSA_RC_INVALID_OBJECTDESCRIPTOR: i32 = 0x51;
pub const BSA_RC_INVALID_QUERYDESCRIPTOR: i32 = 0x53;
pub const BSA_RC_INVALID_VOTE: i32 = 0x0B;
pub const BSA_RC_NO_MATCH: i32 = 0x11;
pub const BSA_RC_NO_MORE_DATA: i32 = 0x12;
pub const BSA_RC_NULL_ARGUMENT: i32 = 0x55;
pub const BSA_RC_OBJECT_NOT_FOUND: i32 = 0x1A;
pub const BSA_RC_TRANSACTION_ABORTED: i32 = 0x20;
pub const BSA_RC_VERSION_NOT_SUPPORTED: i32 = 0x4B;


#[repr(C)]
pub struct ApiVersion {
    pub version:c_short,
    pub release:c_short,
    pub level:c_short,
}

#[repr(C)]
pub struct ObjectOwner {
    pub bsa_object_owner: [c_char; 64],
    pub app_object_owner: [c_char; 64],
}
// TODO
impl ObjectOwner {
    pub fn new() -> ObjectOwner {
        let owner: [c_char; 64] = [0; 64];
        let owner2: [c_char; 64] = [0; 64];
        ObjectOwner { bsa_object_owner: owner, app_object_owner: owner2 }
    }
}
impl Default for ObjectOwner {
    fn default() -> Self {
        ObjectOwner { bsa_object_owner: [0_i8; 64], app_object_owner: [0_i8; 64] }
    }
}
#[repr(C)]
pub struct Tm {
    pub tm_sec: c_int,
    pub tm_min: c_int,
    pub tm_hour: c_int,
    pub tm_mday: c_int,
    pub tm_mon: c_int,
    pub tm_year: c_int,
    pub tm_wday: c_int,
    pub tm_yday: c_int,
    pub tm_isdst: c_int,
    pub tm_gmtoff: c_long,
    pub tm_zone: *const c_char,
}
impl Default for Tm {
    fn default() -> Self {
        Tm {
            tm_zone: ptr::null(),
            tm_sec: 0,
            tm_min: 0,
            tm_hour: 0,
            tm_mday: 0,
            tm_mon: 0,
            tm_year: 0,
            tm_wday: 0,
            tm_yday: 0,
            tm_isdst: 0,
            tm_gmtoff: 0,
        }
    }
}
#[derive(Default)]
#[repr(C)]
pub enum CopyType {
    #[default]
    BSACopyType_ANY = 1,
    BSACopyType_ARCHIVE = 2,
    BSACopyType_BACKUP = 3,
}
#[derive(Default)]
#[repr(C)]
pub enum ObjectType {
    #[default]
    BSAObjectType_ANY = 1,
    BSAObjectType_FILE = 2,
    BSAObjectType_DIRECTORY = 3,
    BSAObjectType_OTHER = 4,
}
#[derive(Default)]
#[repr(C)]
pub enum ObjectStatus{
    #[default]
    BSAObjectStatus_ANY = 1,
    BSAObjectStatus_ACTIVE = 2,
    BSAObjectStatus_INACTIVE = 3,
}
#[repr(C)]
pub struct ObjectName {
    pub object_space_name: [c_char; 1024],
    pub path_name: [c_char; 1024],
}
impl ObjectName {
    pub fn new(space: String, path: String) -> ObjectName {
        info!("----ObjectName new");
        // let mut sp: [i8; 1024] = [0; 1024];
        // let mut pa: [i8; 1024] = [0; 1024];
        // sp.copy_from_slice(unsafe { slice::from_raw_parts(space.as_ptr() as *const i8, space.len()) });
        // pa.copy_from_slice(unsafe { slice::from_raw_parts(path.as_ptr() as *const i8, path.len()) });
        // ObjectName { object_space_name: sp, path_name: pa }
        // let space_cstring = CString::new(space).expect("Failed to create CString");
        let mut sp: [i8; 1024] = [0_i8; 1024];
        // info!("----before slice");
        // sp.copy_from_slice(space_cstring.as_bytes_with_nul().iter().copied().map(|x| x as i8).collect::<Vec<i8>>().as_slice());
        // info!("----after slice");
        for (i, c) in space.chars().enumerate() {
            sp[i] = c as i8;
        }

        // let path_cstring = CString::new(path).expect("Failed to create CString");
        let mut pa: [i8; 1024] = [0_i8; 1024];
        // pa.copy_from_slice(path_cstring.as_bytes_with_nul());
        // pa.copy_from_slice(path_cstring.as_bytes_with_nul().iter().copied().map(|x| x as i8).collect::<Vec<i8>>().as_slice());
        for (i, c) in path.chars().enumerate() {
            pa[i] = c as i8;
        }
        info!("----before ObjectName return");
        ObjectName { object_space_name: sp, path_name: pa }
    }
}
impl Default for ObjectName {
    fn default() -> Self {
        ObjectName { object_space_name: [0_i8; 1024], path_name: [0_i8; 1024] }
    }
}
#[repr(C)]
pub struct ObjectDescriptor {
    pub version:      c_uint,
    pub owner:        ObjectOwner,
    pub obj_name:     ObjectName,
    pub create_time:  Tm,
    pub copy_type:    CopyType,
    pub copy_id:      u64, // two u32 in dll
    pub restore_order:u64, // two u32 in dll
    pub lg_name:      [c_char; 31],
    pub cg_name:      [c_char; 31],
    pub size:         u64,
    pub resource_type:[c_char; 31],
    pub object_type:  ObjectType,
    pub status:       ObjectStatus,
    pub encoding_list:[c_char; 31],
    pub desc:         [c_char; 256],
    pub object_info:  [c_char; 256],
}
impl ObjectDescriptor {
    pub fn new(obj_name: ObjectName, object_info: String) -> ObjectDescriptor {
        let mut info: [i8; 256] = [0_i8; 256];
        info.copy_from_slice(unsafe { slice::from_raw_parts(object_info.as_ptr() as *const i8, object_info.len()) });
        ObjectDescriptor { obj_name: obj_name, object_info: info, ..Default::default()}
    }
}
impl Default for ObjectDescriptor {
    fn default() -> ObjectDescriptor {
        ObjectDescriptor {
            lg_name: [0; 31],
            cg_name: [0; 31],
            resource_type: [0; 31],
            encoding_list: [0; 31],
            desc: [0_i8; 256],
            object_info: [0_i8; 256],
            version: 0,
            owner: ObjectOwner::default(),
            obj_name: ObjectName::default(),
            create_time: Tm::default(),
            copy_type: CopyType::default(),
            copy_id: 0,
            restore_order: 0,
            size: 0,
            object_type: ObjectType::default(),
            status: ObjectStatus::default(),
            // ..Default::default()
        }
    }
}
#[repr(C)]
pub struct QueryDescriptor {
    pub owner:          ObjectOwner,
    pub obj_name:       ObjectName,
    pub create_time_lb: Tm,
    pub create_time_ub: Tm,
    pub expire_time_lb: Tm,
    pub expire_time_ub: Tm,
    pub copy_type:      CopyType,
    pub lg_name:        [c_char; 31],
    pub cg_name:        [c_char; 31],
    pub resource_type:  [c_char; 31],
    pub object_type:    ObjectType,
    pub status:         ObjectStatus,
    pub desc:           [c_char; 100],
}
impl Default for QueryDescriptor {
    fn default() -> QueryDescriptor {
        QueryDescriptor {
            lg_name: [0_i8; 31],
            cg_name: [0_i8; 31],
            resource_type: [0_i8; 31],
            desc: [0_i8; 100],
            owner: ObjectOwner::default(),
            obj_name: ObjectName::default(),
            create_time_lb: Tm::default(),
            create_time_ub: Tm::default(),
            expire_time_lb: Tm::default(),
            expire_time_ub: Tm::default(),
            copy_type: CopyType::default(),
            object_type: ObjectType::default(),
            status: ObjectStatus::default(),
        }
    }
}
#[repr(C)]
#[derive(Debug)]
pub struct DataBlock {
    pub buffer_len:  u32,
    pub num_bytes:   u32,
    pub header_bytes:u32,
    pub share_id:    i32,
    pub share_offset:u32,
    pub buffer_ptr:  *mut c_void,
}
impl Default for DataBlock {
    fn default() -> Self {
        DataBlock {
            buffer_ptr: [0_i8; 1024].as_mut_ptr() as *mut c_void,
            buffer_len: 0,
            num_bytes: 0,
            header_bytes: 0,
            share_id: 0,
            share_offset: 0,
        }
    }
}
fn get_func() -> &'static Symbol<'static, unsafe fn(*mut ApiVersion) -> c_int> {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static FUNC: OnceCell<Symbol<'static, unsafe fn(*mut ApiVersion) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    FUNC.get_or_init(|| unsafe { library.get(b"BSAQueryApiVersion").unwrap() })
}
fn bsa_init(bsa_handle_ptr: *mut i64,
            token_ptr: *mut [c_char; 64],
            object_owner_ptr: *mut ObjectOwner,
            environment_ptr: *const *const c_char) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSAINIT: OnceCell<Symbol<'static, unsafe fn(*mut i64,
        *mut [c_char; 64],
        *mut ObjectOwner,
        *const *const c_char) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSAINIT.get_or_init(|| unsafe { library.get(b"BSAInit").unwrap() });
    unsafe {
        // let rp = &mut *bsa_handle_ptr;
        // let ret = func(rp, token_ptr, object_owner_ptr, environment_ptr) as i32;
        // *bsa_handle_ptr = *rp;
        // ret
        func(bsa_handle_ptr, token_ptr, object_owner_ptr, environment_ptr) as i32
    }
}
fn get_bsa_init() -> &'static Symbol<'static, unsafe fn(*mut i64,
                                                *mut [c_char; 64],
                                                *mut ObjectOwner,
                                                *const *const c_char) -> c_int> {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSAINIT: OnceCell<Symbol<'static, unsafe fn(*mut i64,
        *mut [c_char; 64],
        *mut ObjectOwner,
        *const *const c_char) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    BSAINIT.get_or_init(|| unsafe { library.get(b"BSAInit").unwrap() })
}
fn bsa_begin_txn(bsa_handle: i64) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSABEGINTXN: OnceCell<Symbol<'static, unsafe fn(c_long) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSABEGINTXN.get_or_init(|| unsafe { library.get(b"BSABeginTxn").unwrap() });
    unsafe { func(bsa_handle) as i32}
}
fn get_bsa_begintxn() -> &'static Symbol<'static, unsafe fn(c_long) -> c_int> {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSABEGINTXN: OnceCell<Symbol<'static, unsafe fn(c_long) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    BSABEGINTXN.get_or_init(|| unsafe { library.get(b"BSABeginTxn").unwrap() })
}
fn bsa_create_object(bsa_handle: i64,
                     object_descriptor_ptr: *mut ObjectDescriptor,
                     data_block_ptr: *mut DataBlock) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSACREATEOBJECT: OnceCell<Symbol<'static, unsafe fn(c_long,
                                                               *mut ObjectDescriptor,
                                                               *mut DataBlock) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSACREATEOBJECT.get_or_init(|| unsafe { library.get(b"BSACreateObject").unwrap() });
    info!("----i'm in the bsa_create_object fn");
    unsafe { func(bsa_handle, object_descriptor_ptr, data_block_ptr) as i32}
}
fn get_bsa_createobj() -> &'static Symbol<'static, unsafe fn(c_long,
                                                            *mut ObjectDescriptor,
                                                            *mut DataBlock) -> c_int> {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSACREATEOBJECT: OnceCell<Symbol<'static, unsafe fn(c_long,
        *mut ObjectDescriptor,
        *mut DataBlock) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    BSACREATEOBJECT.get_or_init(|| unsafe { library.get(b"BSACreateObject").unwrap() })
}
fn bsa_send_data(bsa_handle: i64,
                 data_block_ptr: *mut DataBlock) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSASENDDATA: OnceCell<Symbol<'static, unsafe fn(c_long,
                                                            *mut DataBlock) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSASENDDATA.get_or_init(|| unsafe { library.get(b"BSASendData").unwrap() });
    unsafe { func(bsa_handle, data_block_ptr) as i32}
}
fn bsa_get_data(bsa_handle: i64,
                data_block_ptr: *mut DataBlock) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSASENDDATA: OnceCell<Symbol<'static, unsafe fn(c_long,
                                                           *mut DataBlock) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSASENDDATA.get_or_init(|| unsafe { library.get(b"BSAGetData").unwrap() });
    unsafe { func(bsa_handle, data_block_ptr) as i32}
}
fn bsa_query_object(bsa_handle: i64,
                    query_descriptor_ptr: *mut QueryDescriptor,
                    object_descriptor_ptr: *mut ObjectDescriptor) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSAENDDATA: OnceCell<Symbol<'static, unsafe fn(c_long,
                                                          *mut QueryDescriptor,
                                                          *mut ObjectDescriptor) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSAENDDATA.get_or_init(|| unsafe { library.get(b"BSAQueryObject").unwrap() });
    unsafe { func(bsa_handle, query_descriptor_ptr, object_descriptor_ptr) as i32}
}
fn bsa_end_data(bsa_handle: i64) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSAENDDATA: OnceCell<Symbol<'static, unsafe fn(c_long) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSAENDDATA.get_or_init(|| unsafe { library.get(b"BSAEndData").unwrap() });
    unsafe { func(bsa_handle) as i32}
}
fn bsa_get_object(bsa_handle: i64,
                  object_descriptor_ptr: *mut ObjectDescriptor,
                  data_block_ptr: *mut DataBlock) -> i32 {
    static LIBRARY: OnceCell<Library> = OnceCell::new();
    static BSAENDDATA: OnceCell<Symbol<'static, unsafe fn(c_long,
                                                          *mut ObjectDescriptor,
                                                          *mut DataBlock) -> c_int>> = OnceCell::new();
    let library = LIBRARY.get_or_init(|| unsafe { Library::new("/opt/scutech/dbackup3/lib/libxbsa64.so").unwrap() });
    let func = BSAENDDATA.get_or_init(|| unsafe { library.get(b"BSAGetObject").unwrap() });
    unsafe { func(bsa_handle, object_descriptor_ptr, data_block_ptr) as i32}
}
impl XbsaStorage {
    pub fn new(remote: &str, config: XbsaConfig) -> io::Result<XbsaStorage> {
        info!("---------XbsaStorage::new:remote:{}",remote);
        let mut remote = Url::parse(remote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if !remote.path().ends_with('/') {
            let mut new_path = remote.path().to_owned();
            new_path.push('/');
            remote.set_path(&new_path);
        }
        Ok(XbsaStorage { remote, config })
    }
}

const STORAGE_NAME: &str = "xbsa";

#[async_trait]
impl ExternalStorage for XbsaStorage {
    fn name(&self) -> &'static str {
        info!("---------XbsaStorage::name");
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<Url> {
        info!("---------XbsaStorage::url");
        Ok(self.remote.clone())
    }

    async fn write(&self, name: &str, reader: UnpinReader, _content_length: u64) -> io::Result<()> {
        info!("------XbsaStorage::write");
        info!("----write::name: {}", name);
        let mut param = Box::new(ApiVersion{
            version: 80,
            release: 81,
            level  : 82,
        });
        let mut bsa_handle = Box::new(-1 as i64);
        // let bsa_handle_ptr:*mut i64 = &mut -1;
        // let mut bsa_handle_ptr: Box<i64> = Box::new(-1);
        let mut token = [0 as c_char; 64];
        let mut object_owner = ObjectOwner::new();
        let mut host: Vec<c_char> = "BSA_SERVICE_HOST=http://192.168.144.143:50305/".chars().map(|c| c as c_char).collect();
        // let host_ptr = host.as_mut_ptr();
        let mut policy: Vec<c_char> = "BSA_POLICY=0c04b35ecf9711ed8000000c296f9596".chars().map(|c| c as c_char).collect();
        // let policy_ptr = policy.as_mut_ptr();
        // let mut environment: [*const i8; 3] = [std::ptr::null(); 3];
        let host2 = CString::new("BSA_SERVICE_HOST=http://192.168.144.143:50305/").unwrap();
        let policy2 = CString::new("BSA_POLICY=0c04b35ecf9711ed8000000c296f9596").unwrap();
        let func = get_func();
        info!("----ApiVersion return val:{}", unsafe {func(&mut *param)});
        info!("----ApiVersion val:{},{},{}",
        param.version as i16, param.release as i16, param.level as i16);
        unsafe{
            let env = [host2.as_ptr(), policy2.as_ptr(), std::ptr::null()];
            let raw = Box::into_raw(bsa_handle);
            // let init = get_bsa_init();
            info!("----bsainit return val:{}", bsa_init(raw, Box::into_raw(Box::new(token)), Box::into_raw(Box::new(object_owner)), *Box::new(env.as_ptr())));

            bsa_handle = Box::from_raw(raw);
        }
        // bsa_handle = *bsa_handle_ptr;
        info!("----bsa_handle: {}", *bsa_handle);
        // let txn = get_bsa_begintxn();
        info!("----bsabegintxn return: {}", bsa_begin_txn(*bsa_handle));
        let remote_url = self.remote.clone().join(name).unwrap();
        let path = try_convert_to_path(&remote_url);
        let objname = ObjectName::new(String::from("obj"), String::from(path));

        info!("----before ObjectDescriptor");
        unsafe{
            let mut obj_desc = ObjectDescriptor{obj_name: objname, ..ObjectDescriptor::default()};
            // println!("addr:{:?}",obj_desc.obj_name.path_name.as_ptr());
            // info!("----pathname: {:?}", obj_desc.obj_name.path_name);
            // info!("---bsacreateobject return: {}", bsa_create_object(bsa_handle, &mut obj_desc, ptr::null_mut()));
            // let create = get_bsa_createobj();
            let mut blk = DataBlock::default();
            info!("---bsacreateobject return: {}", bsa_create_object(*bsa_handle, &mut obj_desc, &mut blk));
        }
        let mut buffer= [0u8; 1024];
        println!("addr:{:?}",buffer.as_ptr());
        println!("addr:{:p}",buffer.as_ptr());
        // let mut rder = reader.0.compat();
        let mut cp = reader.0.compat();
        let mut bytes_read = cp.read(&mut buffer[..]).await?;
        while bytes_read > 0 {
            unsafe{
                let mut blk = DataBlock::default();

                blk.buffer_len = 1024;
                blk.num_bytes = bytes_read as u32;

                blk.buffer_ptr = buffer.as_mut_ptr() as *mut c_void;
                info!("----bsasenddata return: {}", bsa_send_data(*bsa_handle, &mut blk));
            }

            bytes_read = cp.read(&mut buffer[..]).await?;
        }
        let ret = bsa_end_data(*bsa_handle);
        info!("----bsaenddata return: {}", ret);

        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "---XbsaStorage::write return"))
        }
    }

    fn read(&self, _name: &str) -> ExternalData<'_> {
        info!("---------XbsaStorage::read, name: {}", _name);
        let mut bsa_handle = Box::new(-1 as i64);
        // let bsa_handle_ptr:*mut i64 = &mut -1;
        // let mut bsa_handle_ptr: Box<i64> = Box::new(-1);
        let token = [0 as c_char; 64];
        let object_owner = ObjectOwner::new();
        let host2 = CString::new("BSA_SERVICE_HOST=http://192.168.144.143:50305/").unwrap();
        let policy2 = CString::new("BSA_POLICY=0c04b35ecf9711ed8000000c296f9596").unwrap();
        unsafe{
            let env = [host2.as_ptr(), policy2.as_ptr(), std::ptr::null()];
            let raw = Box::into_raw(bsa_handle);
            // let init = get_bsa_init();
            info!("----bsainit return val:{}", bsa_init(raw, Box::into_raw(Box::new(token)), Box::into_raw(Box::new(object_owner)), *Box::new(env.as_ptr())));

            bsa_handle = Box::from_raw(raw);
        }
        info!("----bsa_handle: {}", *bsa_handle);
        info!("----bsabegintxn return: {}", bsa_begin_txn(*bsa_handle));
        
        let remote_url = self.remote.clone().join(_name).unwrap();
        let path = try_convert_to_path(&remote_url);
        let objname = ObjectName::new(String::from("obj"), String::from(path));
        let mut qd = QueryDescriptor{obj_name: objname, ..Default::default()};
        let mut od = ObjectDescriptor::default();
        info!("----bsaqueryobj return: {}", bsa_query_object(*bsa_handle, &mut qd, &mut od));
        let mut blk = DataBlock::default();
        info!("----bsagetobj return: {}", bsa_get_object(*bsa_handle, &mut od, &mut blk));
        // info!("----blk: {:?}", blk);
        blk.buffer_len = 1024;
        let mut vec: Vec<u8> = Vec::new();
        let mut buf = [0u8; 1024];
        blk.buffer_ptr = buf.as_mut_ptr() as *mut c_void;
        loop {
            let ret = bsa_get_data(*bsa_handle, &mut blk);
            info!("----bsagetdata return: {}", ret);
            let sli: &[u8] = unsafe{
                slice::from_raw_parts_mut(blk.buffer_ptr as *mut u8, blk.num_bytes as usize)
            };
            vec.extend_from_slice(sli);
            if ret == BSA_RC_NO_MORE_DATA {
                break;
            }
            if ret != BSA_RC_SUCCESS {
                let err = io::Error::new(io::ErrorKind::NotFound, format!("ret: {}", ret));
                return Box::new(error_stream(err).into_async_read()) as _
            }
        }

        let cursor = Cursor::new(vec);
        Box::new(AllowStdIo::new(cursor)) as _

        // unimplemented!("currently only XBSA export is implemented")
    }

    fn read_part(&self, _name: &str, _off: u64, _len: u64) -> ExternalData<'_> {
        info!("---------XbsaStorage::read_part");
        unimplemented!("currently only XBSA export is implemented")
    }
}

/// Convert `xbsa:///path` to `/path`
fn try_convert_to_path(url: &Url) -> &str {
    if url.host().is_none() {
        url.path()
    } else {
        url.as_str()
    }
}