#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
extern crate rocket_contrib;

use rocket_contrib::templates::tera::Context;
use rocket_contrib::templates::Template;

#[get("/")]
fn index() -> Template {
    let mut context = Context::new();
    context.insert("value", &26);
    Template::render("index", &context)
}

fn main() {
    rocket::ignite()
        .attach(Template::fairing())
        .mount("/", routes![index])
        .launch();
}
