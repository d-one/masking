import hashlib

import pytest
from masking.utils.hash import hash_string


@pytest.fixture(
    scope="module",
    params=[
        (
            "line",
            hashlib.sha256,
            "secret",
            "utf-8",
            "34c9498d066044916ea29792fdab9a5e3ab1cbe57e8b67324d2a003e9e5bdaec",
        ),
        (
            "line",
            hashlib.sha256,
            "secret",
            "latin-1",
            "34c9498d066044916ea29792fdab9a5e3ab1cbe57e8b67324d2a003e9e5bdaec",
        ),
        (
            "line",
            hashlib.sha256,
            "secret",
            "utf-16",
            "7cb9537844099d2b7fc3eb9fa9b5335e08895fc1ef1de40cd72e77938c97ead8",
        ),
        (
            "line",
            hashlib.sha256,
            "_secret",
            "utf-8",
            "9c05513653e559b14bc2bbc4e1c22b5cb0f3e6b161d5d599b114b4a53e8c059e",
        ),
        (
            "line",
            hashlib.sha256,
            "_secret",
            "latin-1",
            "9c05513653e559b14bc2bbc4e1c22b5cb0f3e6b161d5d599b114b4a53e8c059e",
        ),
        (
            "line",
            hashlib.sha256,
            "_secret",
            "utf-16",
            "e24c804c2b5e75b19bc0eee1edcf14c910f52e3d840ebf17c152bc92965bc0d9",
        ),
        (
            "line",
            hashlib.sha512,
            "secret",
            "utf-8",
            "b9c2c8ed50432951168b953ee45a416365178e5a4727bfd5e5b71575bc8fbbb42badac1f77bc0593b9739437926c8cafccdee25d3a6cc507695df1a679012386",
        ),
        (
            "line",
            hashlib.sha512,
            "secret",
            "latin-1",
            "b9c2c8ed50432951168b953ee45a416365178e5a4727bfd5e5b71575bc8fbbb42badac1f77bc0593b9739437926c8cafccdee25d3a6cc507695df1a679012386",
        ),
        (
            "line",
            hashlib.sha512,
            "secret",
            "utf-16",
            "110eedcfdfc6ba2e7059bfef2934a45cf18a90e61691a85c4dec9615b89464dda2f8529e75227b253e1f78226b6c5df6ca49821f94393113fd0515afc6ce1038",
        ),
        (
            "line",
            hashlib.sha512,
            "_secret",
            "utf-8",
            "995f6e328f4d980f039e059dae311bb19deb75cbd72101d2fc9cbb34a8c2cc2b04290c39cd8a8635d13fb46015af94ddc8afb61a6492c94e85f6964a0b625161",
        ),
        (
            "line",
            hashlib.sha512,
            "_secret",
            "latin-1",
            "995f6e328f4d980f039e059dae311bb19deb75cbd72101d2fc9cbb34a8c2cc2b04290c39cd8a8635d13fb46015af94ddc8afb61a6492c94e85f6964a0b625161",
        ),
        (
            "line",
            hashlib.sha512,
            "_secret",
            "utf-16",
            "7b756ba87b0b4d237fc1e0558a65730f7a462bb6c9aa93e63247f441a3fa77c7f0867660600e3fbc0c8ff41078e2f4aa6fa84ec53f20ba1ae1eddf349dd78d34",
        ),
        ("line", hashlib.md5, "secret", "utf-8", "8ee56dc53e94e219c8e887adc9d42e57"),
        ("line", hashlib.md5, "secret", "latin-1", "8ee56dc53e94e219c8e887adc9d42e57"),
        ("line", hashlib.md5, "secret", "utf-16", "81511ab3981c84138a8ccd6c2ee1e783"),
        ("line", hashlib.md5, "_secret", "utf-8", "55cc1f707096e52b501b3cbcca50f3ee"),
        ("line", hashlib.md5, "_secret", "latin-1", "55cc1f707096e52b501b3cbcca50f3ee"),
        ("line", hashlib.md5, "_secret", "utf-16", "f126a4616242de246bade40757c03023"),
        (
            "line2",
            hashlib.sha256,
            "secret",
            "utf-8",
            "74d70ebd4acd432c1311cfcc2bd131a63160c96686b5d85a26120f411e47f098",
        ),
        (
            "line2",
            hashlib.sha256,
            "secret",
            "latin-1",
            "74d70ebd4acd432c1311cfcc2bd131a63160c96686b5d85a26120f411e47f098",
        ),
        (
            "line2",
            hashlib.sha256,
            "secret",
            "utf-16",
            "9ee3e5630092ec4268ef63aed7d0d71100f31137a1e81e1d6b41404c805358b4",
        ),
        (
            "line2",
            hashlib.sha256,
            "_secret",
            "utf-8",
            "c38a9343a2041b5212515e54d0c01f6de9e81874dd3dec0551461f1c24b68f32",
        ),
        (
            "line2",
            hashlib.sha256,
            "_secret",
            "latin-1",
            "c38a9343a2041b5212515e54d0c01f6de9e81874dd3dec0551461f1c24b68f32",
        ),
        (
            "line2",
            hashlib.sha256,
            "_secret",
            "utf-16",
            "8f3349a4c86af16236196cdef4b9e86a10848de0de115d539e4c23ed97d5dbe0",
        ),
        (
            "line2",
            hashlib.sha512,
            "secret",
            "utf-8",
            "fe428d4df1de7bca156e7fb7c014f2f8749af9ce36776e7675ea52c28f3ab064435ac32a9979537ce8ba0df3c881c7dc093d6705b239b782c3c0f1b3f4029f35",
        ),
        (
            "line2",
            hashlib.sha512,
            "secret",
            "latin-1",
            "fe428d4df1de7bca156e7fb7c014f2f8749af9ce36776e7675ea52c28f3ab064435ac32a9979537ce8ba0df3c881c7dc093d6705b239b782c3c0f1b3f4029f35",
        ),
        (
            "line2",
            hashlib.sha512,
            "secret",
            "utf-16",
            "526657801164d09be4e4d68fd733338b603790663507aa566d4a80c32392dd911569437d8a663fab8e3079365d91a9d46ef1638d3bbe98eeece73d83dbe4b003",
        ),
        (
            "line2",
            hashlib.sha512,
            "_secret",
            "utf-8",
            "8e7be6db38259b89772ba7dc3b33548071e0835439473653b512e9947c6d55b3ed9a7d6acb8174ef9be57231415fb82db8d3caec5c92c75c0d6446ad059dcddd",
        ),
        (
            "line2",
            hashlib.sha512,
            "_secret",
            "latin-1",
            "8e7be6db38259b89772ba7dc3b33548071e0835439473653b512e9947c6d55b3ed9a7d6acb8174ef9be57231415fb82db8d3caec5c92c75c0d6446ad059dcddd",
        ),
        (
            "line2",
            hashlib.sha512,
            "_secret",
            "utf-16",
            "650b9942fcabcb16d238a29d510292cd54816d4d9d3f636e665cc2a0500725bd4f267b8a7046a69ee9a95c4125270ce9af1d03615a2f0f76d5aa3b71c884da97",
        ),
        ("line2", hashlib.md5, "secret", "utf-8", "c08c1cb0507437ffeacadb80a50217d7"),
        ("line2", hashlib.md5, "secret", "latin-1", "c08c1cb0507437ffeacadb80a50217d7"),
        ("line2", hashlib.md5, "secret", "utf-16", "3ffc3256fa42e34f5d3c5a97cce2a457"),
        ("line2", hashlib.md5, "_secret", "utf-8", "dcf2a64231a5621ec38901ee55ea88b1"),
        (
            "line2",
            hashlib.md5,
            "_secret",
            "latin-1",
            "dcf2a64231a5621ec38901ee55ea88b1",
        ),
        ("line2", hashlib.md5, "_secret", "utf-16", "817182e8955bc33c3a6fabd77fc27fd4"),
    ],
)
def inputs_output(request: pytest.FixtureRequest) -> tuple:
    return request.param


def test_hash_string(inputs_output: tuple) -> None:
    line = inputs_output[0]
    hash_function = inputs_output[1]
    secret = inputs_output[2]
    encoding = inputs_output[3]
    expected_output = inputs_output[4]

    output = hash_string(line, secret, hash_function, encoding)

    assert isinstance(output, str), "Output should be a string"
    assert output == expected_output, "Output should be the expected hash"
