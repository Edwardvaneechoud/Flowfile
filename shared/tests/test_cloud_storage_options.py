import pytest

from shared.cloud_storage.utils import create_storage_options_from_boto_credentials


class FakeCredentials:
    def get_frozen_credentials(self):
        class Frozen:
            access_key = "AK"
            secret_key = "SK"
            token = None

        return Frozen()


class FakeSession:
    last_profile = "unset"

    def __init__(self, profile_name=None, region_name=None):
        FakeSession.last_profile = profile_name
        self.region_name = region_name

    def get_credentials(self):
        return FakeCredentials()


@pytest.fixture
def capture_profile(monkeypatch):
    import boto3

    monkeypatch.setattr(boto3, "Session", FakeSession)
    return FakeSession


class TestBotoProfileNormalization:
    """AuthSettingsInput.connection_name defaults to the *string* "None".

    Left unnormalized it reaches boto3 as a profile name and raises ProfileNotFound,
    which is what a brand-new cloud node with aws-cli auth would hit.
    """

    @pytest.mark.parametrize("profile", [None, "", "None"])
    def test_placeholder_profiles_fall_back_to_the_default_profile(self, profile, capture_profile):
        create_storage_options_from_boto_credentials(profile)
        assert capture_profile.last_profile is None

    def test_a_real_profile_name_is_passed_through(self, capture_profile):
        create_storage_options_from_boto_credentials("production")
        assert capture_profile.last_profile == "production"

    def test_returns_frozen_credentials(self, capture_profile):
        options = create_storage_options_from_boto_credentials(None, region_name="eu-west-1")
        assert options["aws_access_key_id"] == "AK"
        assert options["aws_secret_access_key"] == "SK"
        assert options["aws_region"] == "eu-west-1"
