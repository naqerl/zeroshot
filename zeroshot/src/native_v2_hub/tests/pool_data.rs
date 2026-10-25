//! The btrfs data folder of pool servers, against the fake provider and fake tunnels only.

use serde_json::json;

use super::super::data::{DataFs, DataMode, DataSettings};
use super::super::model::HubState;
use super::super::store::migrate;
use super::pool::{add_request, rig};

#[tokio::test]
async fn data_settings_default_to_a_btrfs_file_and_are_validated_and_listed() {
    let rig = rig();
    let folder = rig.directory.path();
    type Case = (&'static str, DataSettings);
    let cases: [Case; 4] = [
        (
            "exclude each other",
            DataSettings {
                size_gb: Some(50),
                volume_gb: Some(50),
                ..DataSettings::default()
            },
        ),
        (
            "--data-size must be at least 5 GB",
            DataSettings {
                size_gb: Some(4),
                ..DataSettings::default()
            },
        ),
        (
            "--data-volume must be 10 to 10240 GB",
            DataSettings {
                volume_gb: Some(9),
                ..DataSettings::default()
            },
        ),
        (
            "--data-volume must be 10 to 10240 GB",
            DataSettings {
                volume_gb: Some(10_241),
                ..DataSettings::default()
            },
        ),
    ];
    for (message, data) in cases {
        let mut request = add_request(folder, 1);
        request.data = data;
        let error = rig.service.add_pool(request).await.expect_err(message);
        assert!(error.to_string().contains(message), "{message}: {error}");
    }
    rig.service
        .add_pool(add_request(folder, 1))
        .await
        .expect("default data");
    let listed = rig.service.list_pools().await;
    assert_eq!(
        listed["pools"][0]["data"],
        json!({"fs": "btrfs", "mode": "file", "sizeGb": null, "volumeGb": null})
    );
    let volume = DataSettings {
        volume_gb: Some(100),
        ..DataSettings::default()
    };
    assert_eq!(volume.mode(), DataMode::Volume);
    assert_eq!(volume.view()["mode"], json!("volume"));
    assert_eq!(volume.view()["volumeGb"], json!(100));
    assert_eq!(DataFs::parse("btrfs").expect("btrfs"), DataFs::Btrfs);
    assert!(
        DataFs::parse("ext4")
            .expect_err("ext4")
            .to_string()
            .contains("not supported")
    );
}

#[tokio::test]
async fn a_v2_state_without_data_settings_gets_the_btrfs_default() {
    let rig = rig();
    rig.service
        .add_pool(add_request(rig.directory.path(), 1))
        .await
        .expect("pool");
    let state: HubState = rig.service.snapshot().await;
    let mut document = serde_json::to_value(&state).expect("state");
    let pool = document["pools"][0].as_object_mut().expect("pool");
    assert!(pool.remove("data").is_some(), "data is stored");
    let migrated = migrate(document.to_string().as_bytes()).expect("migrates");
    assert_eq!(migrated.pools[0].data, DataSettings::default());
    assert_eq!(migrated.pools[0].data.fs, DataFs::Btrfs);
}
