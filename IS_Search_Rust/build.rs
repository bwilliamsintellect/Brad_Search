fn main() {
    #[cfg(windows)]
    {
        let mut resource = winresource::WindowsResource::new();
        resource.set_icon("assets/Q Grey Logo.ico");
        if let Err(error) = resource.compile() {
            println!("cargo:warning=Could not embed Windows icon: {error}");
        }
    }
}
